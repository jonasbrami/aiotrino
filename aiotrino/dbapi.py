# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""

This module implements the Python DBAPI 2.0 as described in
https://www.python.org/dev/peps/pep-0249/ .

Fetch methods returns rows as a list of lists on purpose to let the caller
decide to convert then to a list of tuples.
"""
from __future__ import annotations

import asyncio
import datetime
import math
import uuid
import warnings
import weakref
from collections import OrderedDict, deque
from decimal import Decimal
from threading import Lock
from time import time
from collections.abc import AsyncIterator
from typing import Any, NamedTuple, Optional, Union
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

import aiohttp
from aioitertools import islice as aio_islice

import aiotrino.client
import aiotrino.exceptions
import aiotrino.logging
from aiotrino import constants
from aiotrino.mapper import NoOpRowMapper, RowMapperFactory
from aiotrino.exceptions import (
    DatabaseError,
    DataError,
    Error,
    IntegrityError,
    InterfaceError,
    InternalError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
    Warning,
)
from aiotrino.transaction import NO_TRANSACTION, IsolationLevel, Transaction
from aiotrino.utils import aiter, anext

# Optional pyarrow import
try:
    import pyarrow as pa

    import aiotrino.arrow

    PYARROW_AVAILABLE = True
except ImportError:
    PYARROW_AVAILABLE = False

__all__ = [
    # https://www.python.org/dev/peps/pep-0249/#globals
    "apilevel",
    "threadsafety",
    "paramstyle",
    "connect",
    "Connection",
    "Cursor",
    # https://www.python.org/dev/peps/pep-0249/#exceptions
    "Warning",
    "Error",
    "InterfaceError",
    "DatabaseError",
    "DataError",
    "OperationalError",
    "IntegrityError",
    "InternalError",
    "ProgrammingError",
    "NotSupportedError",
]


apilevel = "2.0"
# Threads may share the module but not connections: a connection, its aiohttp sessions and its cursors are bound to
# the event loop which created them and are not thread-safe.
threadsafety = 1
paramstyle = "qmark"

logger = aiotrino.logging.get_logger(__name__)



class TimeBoundLRUCache:
    """A bounded LRU cache which expires entries after a configured number of seconds.
    Note that expired entries will be evicted only on an attempted access (or through
    the LRU policy)."""

    def __init__(self, capacity: int, ttl_seconds: int):
        self.capacity = capacity
        self.ttl_seconds = ttl_seconds
        self.cache = OrderedDict()
        self.lock = Lock()

    def get(self, key):
        with self.lock:
            if key not in self.cache:
                return None
            value, timestamp = self.cache[key]
            if time() - timestamp > self.ttl_seconds:
                self.cache.pop(key)
                return None
            self.cache.move_to_end(key)
            return value

    def put(self, key, value):
        with self.lock:
            self.cache[key] = value, time()
            self.cache.move_to_end(key)
            if len(self.cache) > self.capacity:
                self.cache.popitem(last=False)

    def __repr__(self):
        return f"LRUCache(capacity: {self.capacity}, ttl: {self.ttl_seconds} seconds, {self.cache})"


must_use_legacy_prepared_statements = TimeBoundLRUCache(1024, 3600)


def _require_module(name: str) -> None:
    import importlib.util

    if importlib.util.find_spec(name) is None:
        raise ImportError(f"{name} is required by this method but is not installed. Install it with: pip install {name}")


def connect(*args, **kwargs):
    """Constructor for creating a connection to the database.

    See class :py:class:`Connection` for arguments.

    :returns: a :py:class:`Connection` object.
    """
    return Connection(*args, **kwargs)


_USE_DEFAULT_ENCODING = object()


class Connection(object):
    """Trino supports transactions and the ability to either commit or rollback
    a sequence of SQL statements. A single query i.e. the execution of a SQL
    statement, can also be cancelled. Transactions are not supported by this
    client implementation yet.

    """

    def __init__(
        self,
        host: str,
        port: int = constants.DEFAULT_PORT,
        user: Optional[str] = None,
        source: str = constants.DEFAULT_SOURCE,
        catalog: str = constants.DEFAULT_CATALOG,
        schema: str = constants.DEFAULT_SCHEMA,
        session_properties: Optional[dict[str, str]] = None,
        http_headers: Optional[dict[str, str]] = None,
        http_scheme: str = constants.HTTP,
        auth: Optional[Any] = constants.DEFAULT_AUTH,
        extra_credential: Optional[list[tuple[str, str]]] = None,
        max_attempts: int = constants.DEFAULT_MAX_ATTEMPTS,
        request_timeout: float = constants.DEFAULT_REQUEST_TIMEOUT,
        isolation_level: IsolationLevel = IsolationLevel.AUTOCOMMIT,
        verify: bool = True,
        http_session: Optional[aiohttp.ClientSession] = None,
        client_tags: Optional[list[str]] = None,
        legacy_primitive_types: bool = False,
        legacy_prepared_statements: Optional[Any] = None,
        roles: Optional[Union[dict[str, str], str]] = None,
        timezone: Optional[str] = None,
        encoding: Union[str, list[str]] = _USE_DEFAULT_ENCODING,
        heartbeat_interval: Optional[float] = constants.DEFAULT_HEARTBEAT_INTERVAL,
    ):
        """
        :param heartbeat_interval: interval in seconds of the heartbeats sent to the coordinator while the client is
            busy downloading spooled segments or slowly consuming the results, so that the query is not abandoned
            after ``query.client.timeout``. ``None`` disables them.
        """
        # Automatically assign http_schema, port based on hostname
        parsed_host = urlparse(host, allow_fragments=False)

        if encoding is _USE_DEFAULT_ENCODING:
            # JSON encodings whose compression library is installed
            encoding = aiotrino.client.available_json_encodings()
            # Only include arrow encoding if pyarrow is available
            if PYARROW_AVAILABLE:
                encoding.append("arrow")

        self.host = host if parsed_host.hostname is None else parsed_host.hostname + parsed_host.path
        self.port = port if parsed_host.port is None else parsed_host.port
        self.user = user
        self.source = source
        self.catalog = catalog
        self.schema = schema
        self.session_properties = session_properties
        self._client_session = aiotrino.client.ClientSession(
            user=user,
            catalog=catalog,
            schema=schema,
            source=source,
            properties=session_properties,
            headers=http_headers,
            transaction_id=NO_TRANSACTION,
            extra_credential=extra_credential,
            client_tags=client_tags,
            roles=roles,
            timezone=timezone,
            encoding=encoding,
            heartbeat_interval=heartbeat_interval,
        )
        # The aiohttp session needs a running event loop, it is created on first use by _create_request()
        self._http_session = http_session
        self._owns_http_session = http_session is None
        # Spooled segments are downloaded with a separate session, without credentials nor default headers
        self._segment_http_session: Optional[aiohttp.ClientSession] = None
        self._closed = False
        self.http_headers = http_headers
        self.http_scheme = http_scheme if not parsed_host.scheme else parsed_host.scheme
        self.auth = auth
        self.extra_credential = extra_credential
        self.max_attempts = max_attempts
        self.request_timeout = request_timeout
        self.client_tags = client_tags

        self._isolation_level = isolation_level
        self._request = None
        self._transaction = None
        self.legacy_primitive_types = legacy_primitive_types
        self.legacy_prepared_statements = legacy_prepared_statements
        self._verify_ssl = verify

    @property
    def isolation_level(self) -> IsolationLevel:
        return self._isolation_level

    @property
    def transaction(self) -> Transaction:
        return self._transaction

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        try:
            await self.commit()
        except Exception:
            await self.rollback()
        else:
            await self.close()

    async def close(self):
        """Close the HTTP session created by this connection. A session passed by the caller is not closed."""
        # TODO cancel outstanding queries?
        self._closed = True
        if self._owns_http_session and self._http_session is not None and not self._http_session.closed:
            await self._http_session.close()
        if self._segment_http_session is not None and not self._segment_http_session.closed:
            await self._segment_http_session.close()

    async def start_transaction(self):
        self._transaction = Transaction(self._create_request())
        await self._transaction.begin()
        return self._transaction

    async def commit(self):
        if self.transaction is None:
            return
        await self._transaction.commit()
        await self._transaction.close()
        self._transaction = None

    async def rollback(self):
        if self.transaction is None:
            raise RuntimeError("no transaction was started")
        await self._transaction.rollback()
        await self._transaction.close()
        self._transaction = None

    def _create_request(self):
        # Creating session here because aiohttp requires it to be created inside the event loop.
        # The Connection only calls this from async functions so therefore it will be in event loop
        if self._closed:
            raise aiotrino.exceptions.InterfaceError("Connection is closed")
        if self._http_session is None:
            self._http_session = aiotrino.client.create_http_session(self._verify_ssl)

        return aiotrino.client.TrinoRequest(
            host=self.host,
            port=self.port,
            client_session=self._client_session,
            http_session=self._http_session,
            http_scheme=self.http_scheme,
            auth=self.auth,
            max_attempts=self.max_attempts,
            request_timeout=self.request_timeout,
            verify=self._verify_ssl,
            segment_http_session_provider=self._get_segment_http_session,
        )

    def _get_segment_http_session(self) -> aiohttp.ClientSession:
        if self._segment_http_session is None or self._segment_http_session.closed:
            self._segment_http_session = aiotrino.client.create_http_session(self._verify_ssl)
        return self._segment_http_session

    async def cursor(self, cursor_style: str = "row", legacy_primitive_types: bool = None) -> "Cursor":
        """Return a new :py:class:`Cursor` object using the connection."""
        if self.isolation_level != IsolationLevel.AUTOCOMMIT:
            if self.transaction is None:
                await self.start_transaction()

        if self.transaction is not None:
            request = self.transaction.request
        else:
            request = self._create_request()

        cursor_class = {
            # Add any custom Cursor classes here
            "segment": SegmentCursor,
            "row": Cursor,
        }.get(cursor_style.lower(), Cursor)

        return cursor_class(
            self,
            request,
            legacy_primitive_types=(
                legacy_primitive_types if legacy_primitive_types is not None else self.legacy_primitive_types
            ),
        )

    async def _use_legacy_prepared_statements(self):
        if self.legacy_prepared_statements is not None:
            return self.legacy_prepared_statements

        value = must_use_legacy_prepared_statements.get((self.host, self.port))
        if value is None:
            try:
                query = aiotrino.client.TrinoQuery(self._create_request(), query="EXECUTE IMMEDIATE 'SELECT 1'")
                await query.execute()
                value = False
            except Exception as e:
                logger.warning(
                    "EXECUTE IMMEDIATE not available for %s:%s; defaulting to legacy prepared statements (%s)",
                    self.host,
                    self.port,
                    e,
                )
                value = True
            must_use_legacy_prepared_statements.put((self.host, self.port), value)
        return value

    def is_closed(self) -> bool:
        return self._closed or (self._http_session is not None and self._http_session.closed)

    @property
    def adbc_current_catalog(self) -> Optional[str]:
        """The current catalog of the session (ADBC API): the catalog of the connection, or the one set by ``USE``."""
        return self._client_session.catalog

    @adbc_current_catalog.setter
    def adbc_current_catalog(self, catalog: Optional[str]) -> None:
        self._client_session.catalog = catalog

    @property
    def adbc_current_db_schema(self) -> Optional[str]:
        """The current schema of the session (ADBC API): the schema of the connection, or the one set by ``USE``."""
        return self._client_session.schema

    @adbc_current_db_schema.setter
    def adbc_current_db_schema(self, schema: Optional[str]) -> None:
        self._client_session.schema = schema

    async def adbc_get_info(self) -> dict[str, Any]:
        """Information about the server and the driver (ADBC API, info names of ``adbc_driver_manager``).

        The server version comes from the coordinator's ``/v1/info`` endpoint.
        """
        request = self._create_request()
        try:
            response = await request._get(
                request.get_url("/v1/info"), headers=request.http_headers, timeout=request.request_client_timeout
            )
            if not response.ok:
                await request.raise_response_error(response)
            server_info = await response.json(content_type=None)
        except (aiohttp.ClientError, asyncio.TimeoutError, ValueError) as e:
            raise OperationalError(f"failed to get the server information: {e}") from e
        finally:
            await request.close()
        return {
            "vendor_name": "Trino",
            "vendor_version": (server_info.get("nodeVersion") or {}).get("version"),
            "vendor_sql": True,
            "vendor_substrait": False,
            "driver_name": "aiotrino",
            "driver_version": aiotrino.__version__,
            "driver_arrow_version": pa.__version__ if PYARROW_AVAILABLE else None,
        }


class DescribeOutput(NamedTuple):
    name: str
    catalog: str
    schema: str
    table: str
    type: str
    type_size: int
    aliased: bool

    @classmethod
    def from_row(cls, row: list[Any]):
        return cls(*row)


class ColumnTypeCode(str):
    """``type_code`` of a :class:`ColumnDescription`: the Trino type of the column (a ``str``, compares equal to the
    type name and works with the DB-API type objects such as ``NUMBER``), which also gives the Trino type signature
    and the Arrow type the column is fetched as (:attr:`arrow_type`, ADBC cursors use Arrow types as type codes)."""

    def __new__(cls, type_name: str, type_signature: Optional[dict[str, Any]] = None):
        type_code = super().__new__(cls, type_name)
        type_code._type_signature = type_signature
        return type_code

    @property
    def type_signature(self) -> Optional[dict[str, Any]]:
        return self._type_signature

    @property
    def arrow_type(self) -> "pa.DataType":
        """The Arrow type of the column (requires pyarrow), see :func:`aiotrino.arrow.to_arrow_type`."""
        if self._type_signature is None:
            raise ValueError(f"No type signature for {self}")
        return aiotrino.arrow.to_arrow_type(self._type_signature)

    def __reduce__(self):
        return ColumnTypeCode, (str(self), self._type_signature)


class ColumnDescription(NamedTuple):
    name: str
    type_code: str
    display_size: int
    internal_size: int
    precision: int
    scale: int
    null_ok: bool

    @classmethod
    def from_column(cls, column: dict[str, Any]):
        type_signature = column["typeSignature"]
        raw_type = type_signature["rawType"]
        arguments = type_signature["arguments"]
        return cls(
            column["name"],  # name
            ColumnTypeCode(column["type"], type_signature),  # type_code
            None,  # display_size
            arguments[0]["value"] if raw_type in constants.LENGTH_TYPES else None,  # internal_size
            arguments[0]["value"] if raw_type in constants.PRECISION_TYPES else None,  # precision
            arguments[1]["value"] if raw_type in constants.SCALE_TYPES else None,  # scale
            None,  # null_ok
        )


class Cursor(object):
    """Database cursor.

    Cursors are not isolated, i.e., any changes done to the database by a
    cursor are immediately visible by other cursors or connections.

    """

    def __init__(
        self,
        connection: Connection,
        request: aiotrino.client.TrinoRequest,
        legacy_primitive_types: bool = False,
    ):
        if not isinstance(connection, Connection):
            raise ValueError("connection must be a Connection object: {}".format(type(connection)))
        self._connection = connection
        self._request = request

        self.arraysize = 1
        self._iterator = None
        self._query = None
        self._legacy_primitive_types = legacy_primitive_types
        # number of rows fetched from the current result
        self._rownumber: Optional[int] = None

    def __aiter__(self):
        return self._iterator

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.close()

    @property
    def connection(self) -> Connection:
        return self._connection

    @property
    def info_uri(self) -> str:
        if self._query is not None:
            return self._query.info_uri
        return None

    @property
    def update_type(self) -> str:
        if self._query is not None:
            return self._query.update_type
        return None

    async def get_description(self) -> list[ColumnDescription]:
        """The description of the columns of the result (PEP 249), fetching the result until the columns are known.
        ``None`` if no query was executed or the query has no result set."""
        if self._query is None or await self._query.get_columns() is None:
            return None

        return [ColumnDescription.from_column(col) for col in await self._query.get_columns()]

    @property
    def description(self) -> Optional[list[ColumnDescription]]:
        """The description of the columns of the result (PEP 249), from the columns already received: ``execute()``
        returns once the first rows (and the columns) are received or the query finished. ``None`` if no query was
        executed or the columns are not known (see :meth:`get_description`).

        ``type_code`` is the Trino type (a :class:`ColumnTypeCode` string), its ``arrow_type`` gives the Arrow type.
        """
        if self._query is None or self._query.columns is None:
            return None
        return [ColumnDescription.from_column(col) for col in self._query.columns]

    @property
    def arrow_schema(self) -> Optional["pa.Schema"]:
        """The Arrow schema of the result, from the columns already received (``None`` if unknown, see
        :meth:`description`). Requires pyarrow."""
        if self._query is None or self._query.columns is None:
            return None
        return aiotrino.arrow.to_arrow_schema(self._query.columns)

    @property
    def rownumber(self) -> Optional[int]:
        """Number of rows fetched from the current result (PEP 249 extension), ``None`` if no query was executed."""
        return self._rownumber

    @property
    def rowcount(self) -> int:
        """The rowcount will be returned for INSERT, UPDATE, DELETE, MERGE
        and CTAS statements based on `update_count` returned by the Trino
        API.

        If the rowcount can't be determined, -1 will be returned.

        Trino cannot reliably determine the number of rows returned for DQL
        queries. For example, the result of a SELECT query is streamed and
        the number of rows is only known when all rows have been retrieved.

        See https://peps.python.org/pep-0249/#rowcount
        """
        if self._query is not None and self._query.update_count is not None:
            return self._query.update_count
        return -1

    @property
    def stats(self) -> dict[Any, Any]:
        if self._query is not None:
            return self._query.stats
        return None

    @property
    def query_id(self) -> str:
        if self._query is not None:
            return self._query.query_id
        return None

    @property
    def query(self) -> str:
        if self._query is not None:
            return self._query.query
        return None

    @property
    def warnings(self) -> list[dict[Any, Any]]:
        if self._query is not None:
            return self._query.warnings
        return None

    def setinputsizes(self, sizes):
        raise aiotrino.exceptions.NotSupportedError

    def setoutputsize(self, size, column):
        raise aiotrino.exceptions.NotSupportedError

    async def _prepare_statement(self, statement: str, name: str) -> None:
        """
        Registers a prepared statement for the provided `operation` with the
        `name` assigned to it.

        :param statement: sql to be executed.
        :param name: name that will be assigned to the prepared statement.
        """
        sql = f"PREPARE {name} FROM {statement}"

        # Send prepare statement. Copy the _request object to avoid poluting the
        # one that is going to be used to execute the actual operation.
        query = aiotrino.client.TrinoQuery(
            self.connection._create_request(),
            query=sql,
            legacy_primitive_types=self._legacy_primitive_types,
        )
        await query.execute()

    def _execute_prepared_statement(self, statement_name: str, params) -> aiotrino.client.TrinoQuery:
        sql = "EXECUTE " + statement_name + " USING " + ",".join(map(self._format_prepared_param, params))
        return aiotrino.client.TrinoQuery(self._request, query=sql, legacy_primitive_types=self._legacy_primitive_types)

    def _execute_immediate_statement(self, statement: str, params) -> aiotrino.client.TrinoQuery:
        """
        Binds parameters and executes a statement in one call.

        :param statement: sql to be executed.
        :param params: parameters to be bound.
        """
        sql = (
            "EXECUTE IMMEDIATE '" + statement.replace("'", "''") + "' USING " + ",".join(map(self._format_prepared_param, params))
        )
        return aiotrino.client.TrinoQuery(
            self.connection._create_request(), query=sql, legacy_primitive_types=self._legacy_primitive_types
        )

    def _format_prepared_param(self, param):
        """
        Formats parameters to be passed in an
        EXECUTE statement.
        """
        if param is None:
            return "NULL"

        if isinstance(param, bool):
            return "true" if param else "false"

        if isinstance(param, int):
            # TODO represent numbers exceeding 64-bit (BIGINT) as DECIMAL
            return "%d" % param

        if isinstance(param, float):
            if param == float("+inf"):
                return "infinity()"
            if param == float("-inf"):
                return "-infinity()"
            if math.isnan(param):
                return "nan()"
            return "DOUBLE '%s'" % param

        if isinstance(param, str):
            return "'%s'" % param.replace("'", "''")

        if isinstance(param, (bytes, bytearray)):
            return "X'%s'" % param.hex()

        if isinstance(param, datetime.datetime) and param.tzinfo is None:
            datetime_str = param.strftime("%Y-%m-%d %H:%M:%S.%f")
            return "TIMESTAMP '%s'" % datetime_str

        if isinstance(param, datetime.datetime) and param.tzinfo is not None:
            datetime_str = param.strftime("%Y-%m-%d %H:%M:%S.%f")
            # named timezones
            if isinstance(param.tzinfo, ZoneInfo):
                return "TIMESTAMP '%s %s'" % (datetime_str, param.tzinfo.key)
            # offset-based timezones
            return "TIMESTAMP '%s %s'" % (datetime_str, param.tzinfo.tzname(param))

        # We can't calculate the offset for a time without a point in time
        if isinstance(param, datetime.time) and param.tzinfo is None:
            time_str = param.strftime("%H:%M:%S.%f")
            return "TIME '%s'" % time_str

        if isinstance(param, datetime.time) and param.tzinfo is not None:
            time_str = param.strftime("%H:%M:%S.%f")
            # named timezones
            if isinstance(param.tzinfo, ZoneInfo):
                utc_offset = datetime.datetime.now(tz=param.tzinfo).strftime("%z")
                return "TIME '%s %s:%s'" % (time_str, utc_offset[:3], utc_offset[3:])
            # offset-based timezones
            return "TIME '%s %s'" % (time_str, param.strftime("%Z")[3:])

        if isinstance(param, datetime.date):
            date_str = param.strftime("%Y-%m-%d")
            return "DATE '%s'" % date_str

        if isinstance(param, list):
            return "ARRAY[%s]" % ",".join(map(self._format_prepared_param, param))

        if isinstance(param, tuple):
            return "ROW(%s)" % ",".join(map(self._format_prepared_param, param))

        if isinstance(param, dict):
            keys = list(param.keys())
            values = [param[key] for key in keys]
            return "MAP({}, {})".format(self._format_prepared_param(keys), self._format_prepared_param(values))

        if isinstance(param, uuid.UUID):
            return "UUID '%s'" % param

        if isinstance(param, Decimal):
            return "DECIMAL '%s'" % format(param, "f")

        raise aiotrino.exceptions.NotSupportedError("Query parameter of type '%s' is not supported." % type(param))

    async def _deallocate_prepared_statement(self, statement_name: str) -> None:
        sql = "DEALLOCATE PREPARE " + statement_name
        query = aiotrino.client.TrinoQuery(
            self.connection._create_request(), query=sql, legacy_primitive_types=self._legacy_primitive_types
        )
        await query.execute()

    def _generate_unique_statement_name(self) -> str:
        return "st_" + uuid.uuid4().hex.replace("-", "")

    async def execute(self, operation, params=None):
        self._rownumber = 0
        if params:
            if not isinstance(params, (list, tuple)):
                raise ProgrammingError("params must be a list or tuple containing the query parameter values")

            if await self.connection._use_legacy_prepared_statements():
                statement_name = self._generate_unique_statement_name()
                await self._prepare_statement(operation, statement_name)

                try:
                    # Send execute statement and assign the return value to `results`
                    # as it will be returned by the function
                    self._query = self._execute_prepared_statement(statement_name, params)
                    self._iterator = aiter(await self._query.execute())
                finally:
                    # Send deallocate statement
                    # At this point the query can be deallocated since it has already
                    # been executed
                    # TODO: Consider caching prepared statements if requested by caller
                    await self._deallocate_prepared_statement(statement_name)
            else:
                self._query = self._execute_immediate_statement(operation, params)
                self._iterator = aiter(await self._query.execute())

        else:
            self._query = aiotrino.client.TrinoQuery(
                self._request, query=operation, legacy_primitive_types=self._legacy_primitive_types
            )
            self._iterator = aiter(await self._query.execute())
        return self

    async def executemany(self, operation, seq_of_params):
        """
        PEP-0249: Prepare a database operation (query or command) and then
        execute it against all parameter sequences or mappings found in the sequence seq_of_parameters.
        Modules are free to implement this method using multiple calls to
        the .execute() method or by using array operations to have the
        database process the sequence as a whole in one call.

        Use of this method for an operation which produces one or more result
        sets constitutes undefined behavior, and the implementation is permitted (but not required)
        to raise an exception when it detects that a result set has been created by an invocation of the operation.

        The same comments as for .execute() also apply accordingly to this method.

        Return values are not defined.
        """
        for parameters in seq_of_params[:-1]:
            await self.execute(operation, parameters)
            await self.fetchall()
            if self._query.update_type is None:
                raise NotSupportedError("Query must return update type")
        if seq_of_params:
            await self.execute(operation, seq_of_params[-1])
        else:
            await self.execute(operation)
        return self

    async def fetchone(self) -> Optional[list[Any]]:
        """

        PEP-0249: Fetch the next row of a query result set, returning a single
        sequence, or None when no more data is available.

        An Error (or subclass) exception is raised if the previous call to
        .execute*() did not produce any result set or no call was issued yet.
        """

        if self._iterator is None:
            raise ProgrammingError("execute() must be called before fetching results")
        try:
            row = await anext(self._iterator)
        except StopAsyncIteration:
            return None
        except aiotrino.exceptions.HttpError as err:
            raise aiotrino.exceptions.OperationalError(str(err)) from None
        if isinstance(row, aiotrino.client.DecodableSegment):
            self._rownumber = (self._rownumber or 0) + int((row.segment.metadata or {}).get("rowsCount", 0))
        else:
            self._rownumber = (self._rownumber or 0) + 1
        return row

    async def fetchmany(self, size: Optional[int] = None) -> list[list[Any]]:
        """
        PEP-0249: Fetch the next set of rows of a query result, returning a
        sequence of sequences (e.g. a list of tuples). An empty sequence is
        returned when no more rows are available.

        The number of rows to fetch per call is specified by the parameter. If
        it is not given, the cursor's arraysize determines the number of rows
        to be fetched. The method should try to fetch as many rows as indicated
        by the size parameter. If this is not possible due to the specified
        number of rows not being available, fewer rows may be returned.

        An Error (or subclass) exception is raised if the previous call to
        .execute*() did not produce any result set or no call was issued yet.

        Note there are performance considerations involved with the size
        parameter. For optimal performance, it is usually best to use the
        .arraysize attribute. If the size parameter is used, then it is best
        for it to retain the same value from one .fetchmany() call to the next.
        """

        if size is None:
            size = self.arraysize

        return [row async for row in aio_islice(aiter(self.fetchone, None), size)]

    async def describe(self, sql: str) -> list[DescribeOutput]:
        """
        List the output columns of a SQL statement, including the column name (or alias), catalog, schema, table, type,
        type size in bytes, and a boolean indicating if the column is aliased.

        :param sql: SQL statement
        """
        statement_name = self._generate_unique_statement_name()
        await self._prepare_statement(sql, statement_name)
        try:
            sql = f"DESCRIBE OUTPUT {statement_name}"
            self._query = aiotrino.client.TrinoQuery(
                self._request,
                query=sql,
                legacy_primitive_types=self._legacy_primitive_types,
            )
            result = await self._query.execute()
        finally:
            await self._deallocate_prepared_statement(statement_name)

        return [DescribeOutput.from_row(x) async for x in result]

    def genall(self):
        return self._query.result

    async def fetchall(self) -> list[list[Any]]:
        return [row async for row in aiter(self.fetchone, None)]

    async def cancel(self):
        if self._query is None:
            return
        await self._query.cancel()

    async def adbc_cancel(self) -> None:
        """Cancel the running query (ADBC API), alias of :meth:`cancel`."""
        await self.cancel()

    async def adbc_get_info(self) -> dict[str, Any]:
        """See :meth:`Connection.adbc_get_info`."""
        return await self.connection.adbc_get_info()

    @property
    def adbc_current_catalog(self) -> Optional[str]:
        """See :attr:`Connection.adbc_current_catalog`."""
        return self.connection.adbc_current_catalog

    @property
    def adbc_current_db_schema(self) -> Optional[str]:
        """See :attr:`Connection.adbc_current_db_schema`."""
        return self.connection.adbc_current_db_schema

    async def close(self):
        if self._query is not None:
            await self._query.aclose()
        await self.cancel()
        await self._request.close()

    def _check_executed(self) -> None:
        if self._query is None or self._iterator is None:
            raise ProgrammingError("execute() must be called before fetching results")

    async def _arrow_schema(self) -> "pa.Schema":
        """Arrow schema of the result, from the column metadata (see :func:`aiotrino.arrow.to_arrow_schema`)."""
        self._check_executed()
        columns = await self._query.get_columns()
        return aiotrino.arrow.to_arrow_schema(columns) if columns else pa.schema([])

    async def fetch_arrow_table(self) -> "pa.Table":
        """Fetch the rest of the result as a ``pyarrow.Table`` (ADBC API). Always returns a table: an empty result
        gives an empty table with the schema of the result.

        This cursor converts the rows to Arrow: use ``cursor("segment")`` to receive the Arrow segments as is.
        """
        aiotrino.arrow.require_pyarrow()
        schema = await self._arrow_schema()
        batches = [batch async for batch in self._row_record_batches(schema)]
        return pa.Table.from_batches(batches, schema=schema)

    async def _row_record_batches(self, schema: "pa.Schema"):
        """The remaining rows of the result as record batches."""
        columns = self._query.columns
        mapper = None
        if isinstance(self._query._row_mapper, NoOpRowMapper) and columns:
            # legacy_primitive_types: Arrow needs the values mapped to Python types
            mapper = RowMapperFactory().create(columns=columns, legacy_primitive_types=False)

        def to_batch(rows):
            if mapper is not None:
                rows = mapper.map(rows)
            return aiotrino.arrow.rows_to_record_batch(rows, columns, schema)

        rows: list[Any] = []
        while (row := await self.fetchone()) is not None:
            rows.append(row)
            if len(rows) >= _DIRECT_PROTOCOL_ROWS_PER_TABLE:
                batch, rows = await asyncio.to_thread(to_batch, rows), []
                yield batch
        if rows:
            yield await asyncio.to_thread(to_batch, rows)

    async def fetch_record_batches(self, prefetch: Optional[int] = None) -> AsyncIterator["pa.RecordBatch"]:
        """Yield the rest of the result as ``pyarrow.RecordBatch`` objects, in order (ADBC's
        ``fetch_record_batch()`` as an async iterator).

        This cursor converts the rows to Arrow (the spooled segments are already prefetched while iterating, so
        ``prefetch`` is ignored): use ``cursor("segment")`` to receive the Arrow segments as is.
        """
        aiotrino.arrow.require_pyarrow()
        schema = await self._arrow_schema()
        async for batch in self._row_record_batches(schema):
            yield batch

    async def fetchallarrow(self) -> "pa.Table":
        """Alias of :meth:`fetch_arrow_table` (ADBC API)."""
        return await self.fetch_arrow_table()

    async def fetch_df(self, *, arrow_dtypes: bool = False, **to_pandas_kwargs) -> "pandas.DataFrame":  # noqa: F821
        """Fetch the rest of the result as a pandas DataFrame (ADBC API): ``fetch_arrow_table().to_pandas()``.

        Columns pandas cannot convert without losing data (TIME(9)/TIME(12) values) use ``pandas.ArrowDtype``.

        :param arrow_dtypes: use ``pandas.ArrowDtype`` for every column (``types_mapper=pandas.ArrowDtype``)
        :param to_pandas_kwargs: passed to ``pyarrow.Table.to_pandas()``
        """
        _require_module("pandas")
        table = await self.fetch_arrow_table()
        return await asyncio.to_thread(
            aiotrino.arrow.table_to_pandas, table, arrow_dtypes=arrow_dtypes, **to_pandas_kwargs
        )

    async def fetch_polars(self) -> "polars.DataFrame":  # noqa: F821
        """Fetch the rest of the result as a polars DataFrame (requires polars).

        polars has no year-month interval type: INTERVAL YEAR TO MONTH values are returned as their number of
        months (Int32).
        """
        _require_module("polars")
        table = await self.fetch_arrow_table()
        return await asyncio.to_thread(aiotrino.arrow.table_to_polars, table)

    async def fetchall_arrow(self) -> "pa.Table":
        """Deprecated alias of :meth:`fetch_arrow_table`."""
        warnings.warn("fetchall_arrow() is deprecated, use fetch_arrow_table()", DeprecationWarning, stacklevel=2)
        return await self.fetch_arrow_table()

    async def fetchone_arrow(self) -> Optional["pa.Table"]:
        raise NotImplementedError(
            "fetchone_arrow is not implemented for this cursor type. "
            "Use SegmentCursor for fetching results as Apache Arrow Table."
        )


class SegmentCursor(Cursor):
    """Cursor returning the spooled segments of the result (``fetchone()`` returns a
    :class:`aiotrino.client.DecodableSegment`), and the result as Apache Arrow tables (``fetch_arrow_table()``,
    ``fetchone_arrow()``).

    The Arrow methods work with every response of the server: Arrow segments (``arrow``, ``arrow+zstd`` encodings) are
    decoded as is, segments of the JSON encodings and rows of the direct protocol (server without spooling) are
    converted to Arrow with the schema given by :func:`aiotrino.arrow.to_arrow_schema`.
    """

    def __init__(self, connection, request, legacy_primitive_types: bool = False):
        super().__init__(connection, request, legacy_primitive_types=legacy_primitive_types)
        if self.connection._client_session.encoding is None:
            raise ValueError("SegmentCursor can only be used if encoding is set on the connection")
        self._arrow_reader: Optional[_ArrowReader] = None

    async def execute(self, operation, params=None):
        if params:
            # TODO: refactor code to allow for params to be supported
            raise ValueError("params not supported")

        await self._close_arrow_reader()
        self._rownumber = 0
        self._query = aiotrino.client.TrinoQuery(
            self._request,
            query=operation,
            legacy_primitive_types=self._legacy_primitive_types,
            fetch_mode="segments",
        )
        self._iterator = aiter(await self._query.execute())
        return self

    def _get_arrow_reader(self) -> _ArrowReader:
        aiotrino.arrow.require_pyarrow()
        if self._query is None or self._iterator is None:
            raise ProgrammingError("execute() must be called before fetching results")
        if self._arrow_reader is None:
            self._arrow_reader = _ArrowReader(self._query, self._iterator)
        return self._arrow_reader

    async def _close_arrow_reader(self) -> None:
        if self._arrow_reader is not None:
            await self._arrow_reader.aclose()
            self._arrow_reader = None

    async def fetchone_arrow(self) -> Optional["pa.Table"]:
        """Fetch the next segment of the result as an Apache Arrow Table, ``None`` when the result is exhausted.

        The next segments are downloaded and decoded in the background (up to ``DEFAULT_SEGMENT_PREFETCH``).
        """
        table = await self._get_arrow_reader().next_table(constants.DEFAULT_SEGMENT_PREFETCH)
        if table is not None:
            self._rownumber = (self._rownumber or 0) + table.num_rows
        return table

    async def fetch_record_batches(self, prefetch: Optional[int] = None) -> AsyncIterator["pa.RecordBatch"]:
        """Yield the rest of the result as ``pyarrow.RecordBatch`` objects, in segment order (ADBC's
        ``fetch_record_batch()`` as an async iterator)::

            async for batch in cursor.fetch_record_batches(prefetch=4):
                process(batch)

        Up to ``prefetch`` segments (default ``DEFAULT_SEGMENT_PREFETCH``) are downloaded and decoded (in worker
        threads) ahead while the caller processes the batches, so memory stays bounded. Each segment is acknowledged
        once decoded. Heartbeats keep the query alive while the caller is slow. Leaving the loop early (or closing
        the iterator) cancels the downloads in progress: those segments are loaded again if the result is read later.
        """
        reader = self._get_arrow_reader()
        prefetch = prefetch if prefetch is not None else constants.DEFAULT_SEGMENT_PREFETCH
        try:
            while (table := await reader.next_table(prefetch)) is not None:
                self._rownumber = (self._rownumber or 0) + table.num_rows
                for batch in table.to_batches():
                    yield batch
        finally:
            # An iterator left with "break" is closed later, when garbage collected: do not cancel the downloads of
            # another call reading the result in the meantime.
            if not reader.active:
                await reader.cancel_pending()

    async def fetch_arrow_table(self) -> "pa.Table":
        """Fetch the (rest of the) result as a ``pyarrow.Table`` (ADBC API). Always returns a table, empty if there
        are no rows (with the schema of the result).

        Up to ``MAX_PARALLEL_SEGMENT_RETRIEVAL`` segments are downloaded concurrently and decoded in worker threads
        (pyarrow releases the GIL). The whole result is materialized in memory.
        """
        reader = self._get_arrow_reader()
        tables = []
        while (table := await reader.next_table(constants.MAX_PARALLEL_SEGMENT_RETRIEVAL)) is not None:
            self._rownumber = (self._rownumber or 0) + table.num_rows
            tables.append(table)
        return await reader.concat(tables)

    async def close(self):
        await self._close_arrow_reader()
        await super().close()


# rows of the direct protocol are converted to Arrow by groups of this size
_DIRECT_PROTOCOL_ROWS_PER_TABLE = 65_536


class _ArrowReader:
    """Reads the result of a ``segments`` fetch mode query as Arrow tables, one segment at a time and in order.

    Up to ``prefetch`` segments are downloaded and decoded concurrently, in background tasks. When fetching fails or
    is cancelled, the tasks in progress are cancelled and awaited (nothing keeps running in the background) and the
    segments which were not loaded are loaded again by the next call: no segment is skipped. Segments are acknowledged
    after they are decoded.
    """

    def __init__(self, query: aiotrino.client.TrinoQuery, items) -> None:
        self._query = query
        # the cursor's iterator: DecodableSegment items, or rows (direct protocol)
        self._items = items
        # [unit, task loading it or None] in result order; a unit is a segment or a list of rows
        self._pending: deque[list[Any]] = deque()
        self._rows: list[Any] = []
        self._exhausted = False
        self._closed = False
        self._schema = None
        self._decoders: dict[str, Any] = {}
        self._mapper = None
        self._keep_alive_task: Optional[asyncio.Task] = None
        # number of calls reading the result
        self._active = 0

    @property
    def schema(self):
        """Arrow schema of the result, from the column metadata (empty schema if there are no columns)."""
        if self._schema is None:
            columns = self._query.columns
            self._schema = aiotrino.arrow.to_arrow_schema(columns) if columns else pa.schema([])
        return self._schema

    async def next_table(self, prefetch: int) -> Optional["pa.Table"]:
        """The rows of the next segment (or group of rows) as a table, ``None`` at the end of the result."""
        if self._closed:
            raise ProgrammingError("the result was closed")
        self._start_keep_alive()
        self._active += 1
        try:
            await self._fill(prefetch)
            if not self._pending:
                await self._stop_keep_alive()
                return None
            entry = self._pending[0]
            async with self._query.keep_alive():
                while True:
                    if entry[1] is None:
                        entry[1] = self._start(entry[0])
                    task = entry[1]
                    try:
                        table = await task
                        break
                    except asyncio.CancelledError:
                        if asyncio.current_task().cancelling():
                            raise
                        # the task was cancelled by someone else (e.g. a fetch_record_batches() iterator closed
                        # concurrently): load the segment again
                        if entry[1] is task:
                            entry[1] = None
        except BaseException:
            await self.cancel_pending()
            raise
        finally:
            self._active -= 1
        self._pending.popleft()
        await self._fill(prefetch)
        return table

    async def concat(self, tables: list["pa.Table"]) -> "pa.Table":
        if not tables:
            if self._query.columns is None:
                await self._query.get_columns()
            return self.schema.empty_table()
        if len(tables) == 1:
            return tables[0]
        return pa.concat_tables(tables)

    async def _fill(self, prefetch: int) -> None:
        """Start loading the next units until ``prefetch`` units are pending."""
        while not self._exhausted and len(self._pending) < max(1, prefetch):
            try:
                item = await anext(self._items)
            except StopAsyncIteration:
                self._exhausted = True
                self._flush_rows()
                break
            if isinstance(item, aiotrino.client.DecodableSegment):
                self._flush_rows()
                self._pending.append([item, self._start(item)])
            else:
                # a row of the direct protocol (server without spooling)
                self._rows.append(item)
                if len(self._rows) >= _DIRECT_PROTOCOL_ROWS_PER_TABLE:
                    self._flush_rows()
        for entry in self._pending:
            if entry[1] is None:
                entry[1] = self._start(entry[0])

    def _flush_rows(self) -> None:
        if self._rows:
            rows, self._rows = self._rows, []
            self._pending.append([rows, self._start(rows)])

    def _start(self, unit) -> asyncio.Task:
        if isinstance(unit, aiotrino.client.DecodableSegment):
            coroutine = self._load_segment(unit)
        else:
            coroutine = asyncio.to_thread(self._rows_to_table, unit)
        task = asyncio.get_running_loop().create_task(coroutine)
        task.add_done_callback(aiotrino.client._retrieve_exception)
        return task

    async def _load_segment(self, segment: aiotrino.client.DecodableSegment) -> "pa.Table":
        encoding = segment.encoding
        if encoding in ("arrow", "arrow+zstd"):
            decode = self._decode_arrow
        elif encoding in aiotrino.client.ENCODINGS:
            decoder = self._decoders.get(encoding)
            if decoder is None:
                decoder = self._decoders[encoding] = aiotrino.client.CompressedQueryDataDecoderFactory(
                    self._row_mapper()
                ).create(encoding)

            def decode(data, metadata):
                return self._rows_to_table(decoder.decode(data, metadata), mapped=True)
        else:
            raise NotSupportedError(f"Cannot convert segments of encoding {encoding!r} to Arrow")
        return await aiotrino.client._load_segment(segment, decode, in_thread=True)

    def _decode_arrow(self, data: bytes, metadata) -> "pa.Table":
        schema, batches = aiotrino.arrow.read_ipc_stream(data)
        table = pa.Table.from_batches(batches, schema=schema)
        expected = self.schema
        if table.schema.equals(expected):
            # same names and types: use the schema with the Trino types in the field metadata (zero-copy)
            table = pa.Table.from_arrays(table.columns, schema=expected)
        return table

    def _row_mapper(self):
        if self._mapper is None:
            mapper = self._query._row_mapper
            if mapper is None or isinstance(mapper, NoOpRowMapper):
                # legacy_primitive_types: Arrow needs the values mapped to Python types
                mapper = RowMapperFactory().create(columns=self._query.columns, legacy_primitive_types=False)
            self._mapper = mapper
        return self._mapper

    def _rows_to_table(self, rows: list[Any], mapped: bool = False) -> "pa.Table":
        if not mapped and isinstance(self._query._row_mapper, NoOpRowMapper):
            rows = self._row_mapper().map(rows)
        batch = aiotrino.arrow.rows_to_record_batch(rows, self._query.columns, self.schema)
        return pa.Table.from_batches([batch])

    async def cancel_pending(self) -> None:
        """Cancel the units being loaded; they are loaded again by the next call. Loaded units are kept."""
        tasks = []
        for entry in self._pending:
            task = entry[1]
            if task is None:
                continue
            if not task.done():
                task.cancel()
                tasks.append(task)
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        for entry in self._pending:
            task = entry[1]
            if task is not None and task.done() and (task.cancelled() or task.exception() is not None):
                entry[1] = None

    @property
    def active(self) -> bool:
        """Whether a call is reading the result."""
        return self._active > 0

    def _start_keep_alive(self) -> None:
        """Send heartbeats while the result is being read, even if the caller is slow between two calls. The task
        stops when the reader is closed or garbage collected, or when the query has finished."""
        if self._keep_alive_task is None and self._query.heartbeat_interval:
            self._keep_alive_task = asyncio.get_running_loop().create_task(
                _keep_alive(weakref.ref(self), self._query.heartbeat_interval)
            )

    async def _stop_keep_alive(self) -> None:
        task, self._keep_alive_task = self._keep_alive_task, None
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def aclose(self) -> None:
        self._closed = True
        tasks = [entry[1] for entry in self._pending if entry[1] is not None]
        self._pending.clear()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await self._stop_keep_alive()

    def __del__(self) -> None:
        tasks = [entry[1] for entry in getattr(self, "_pending", ())] + [getattr(self, "_keep_alive_task", None)]
        for task in tasks:
            if task is not None and not task.done():
                try:
                    task.cancel()
                except RuntimeError:  # event loop already closed
                    pass


async def _keep_alive(reader_ref: weakref.ref, interval: float) -> None:
    while True:
        await asyncio.sleep(interval / 2)
        reader = reader_ref()
        if reader is None or reader._closed or reader._query.finished or reader._query.cancelled:
            return
        reader._query.heartbeat()
        del reader


Date = datetime.date
Time = datetime.time
Timestamp = datetime.datetime
DateFromTicks = datetime.date.fromtimestamp
TimestampFromTicks = datetime.datetime.fromtimestamp


def TimeFromTicks(ticks):
    return datetime.time(*datetime.localtime(ticks)[3:6])


def Binary(string):
    return string.encode("utf-8")


class DBAPITypeObject:
    def __init__(self, *values):
        self.values = [v.lower() for v in values]

    def __eq__(self, other):
        return other.lower() in self.values


STRING = DBAPITypeObject("VARCHAR", "CHAR", "VARBINARY", "JSON", "IPADDRESS")

BINARY = DBAPITypeObject("ARRAY", "MAP", "ROW", "HyperLogLog", "P4HyperLogLog", "QDigest")

NUMBER = DBAPITypeObject("BOOLEAN", "TINYINT", "SMALLINT", "INTEGER", "BIGINT", "REAL", "DOUBLE", "DECIMAL")

DATETIME = DBAPITypeObject(
    "DATE",
    "TIME",
    "TIME WITH TIME ZONE",
    "TIMESTAMP",
    "TIMESTAMP WITH TIME ZONE",
    "INTERVAL YEAR TO MONTH",
    "INTERVAL DAY TO SECOND",
)

ROWID = DBAPITypeObject()  # nothing indicates row id in Trino
