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

This module implements the Trino protocol to submit SQL statements, track
their state and retrieve their result as described in
https://github.com/trinodb/trino/wiki/HTTP-Protocol
and Trino source code.

The outline of a query is:
- Send HTTP POST to the coordinator
- Retrieve HTTP response with ``nextUri``
- Get status of the query execution by sending a HTTP GET to the coordinator

Trino queries are managed by the ``TrinoQuery`` class. HTTP requests are
managed by the ``TrinoRequest`` class. the status of a query is represented
by ``TrinoStatus`` and the result by ``TrinoResult``.


The main interface is :class:`TrinoQuery`: ::

    >> request = TrinoRequest(host='coordinator', port=8080, user='test')
    >> query =  TrinoQuery(request, sql)
    >> rows = list(query.execute())
"""

from __future__ import annotations

import abc
import asyncio
import base64
import contextlib
import copy
import functools
import os
import random
import re
import ssl
import threading
import time
import urllib.parse
import warnings
from abc import abstractmethod
from collections import deque
from asyncio import sleep
from dataclasses import dataclass
from datetime import datetime
from email.utils import parsedate_to_datetime
from enum import Enum
from collections.abc import AsyncIterator, Iterable, Iterator
from typing import Any, Callable, Literal, Optional, TypedDict, Union, cast
from zoneinfo import ZoneInfo

import aiohttp
import aiohttp.client_exceptions
from multidict import CIMultiDict
from tzlocal import get_localzone_name  # type: ignore
from yarl import URL

# The compression libraries are optional: a missing or broken library disables the corresponding encoding
try:
    import lz4.block
except ImportError as err:
    _LZ4_ERROR: Optional[str] = str(err)
else:
    _LZ4_ERROR = None

try:
    import zstandard
except ImportError as err:
    _ZSTD_ERROR: Optional[str] = str(err)
else:
    _ZSTD_ERROR = None

try:
    # faster JSON decoding, as in upstream trino-python-client (not available on PyPy)
    import orjson

    def _json_loads(data: Union[bytes, str]) -> Any:
        return orjson.loads(data)

except ImportError:
    import json

    def _json_loads(data: Union[bytes, str]) -> Any:
        return json.loads(data)

import aiotrino.logging
from aiotrino import constants, exceptions
from aiotrino._version import __version__
from aiotrino.auth import Authentication
from aiotrino.exceptions import TrinoExternalError, TrinoQueryError, TrinoUserError
from aiotrino.mapper import RowMapper, RowMapperFactory

__all__ = [
    "ClientSession",
    "TrinoQuery",
    "TrinoRequest",
    "PROXIES",
    "DecodableSegment",
    "SpooledSegment",
    "InlineSegment",
    "Segment",
]


logger = aiotrino.logging.get_logger(__name__)


MAX_ATTEMPTS = constants.DEFAULT_MAX_ATTEMPTS
SOCKS_PROXY = os.environ.get("SOCKS_PROXY")
if SOCKS_PROXY:
    PROXIES = {"http": "socks5://" + SOCKS_PROXY, "https": "socks5://" + SOCKS_PROXY}
else:
    PROXIES = None

_HEADER_EXTRA_CREDENTIAL_KEY_REGEX = re.compile(r"^\S[^\s=]*$")

ROLE_PATTERN = re.compile(r"^ROLE\{(.*)\}$")

# JSON encodings of the spooling protocol, in order of preference
ENCODINGS = ["json+zstd", "json+lz4", "json"]
# compression codec -> error message, for the codecs whose library could not be imported
CODECS_UNAVAILABLE: dict[str, str] = {}
if _LZ4_ERROR:
    CODECS_UNAVAILABLE["lz4"] = _LZ4_ERROR
if _ZSTD_ERROR:
    CODECS_UNAVAILABLE["zstd"] = _ZSTD_ERROR


def available_json_encodings() -> list[str]:
    """The JSON encodings whose compression codec is installed."""
    return [enc for enc in ENCODINGS if (enc.split("+")[1] if "+" in enc else None) not in CODECS_UNAVAILABLE]


_REDIRECT_STATUSES = (301, 302, 303, 307, 308)
_MAX_SPOOLING_REDIRECTS = 10


class ClientSession:
    """
    Manage the current Client Session properties of a specific connection. This class is thread-safe.

    :param user: associated with the query. It is useful for access control
                 and query scheduling.
    :param authorization_user: associated with the query. It is useful for access control
                               and query scheduling.
    :param source: associated with the query. It is useful for access
                   control and query scheduling.
    :param catalog: to query. The *catalog* is associated with a Trino
                    connector. This variable sets the default catalog used
                    by SQL statements. For example, if *catalog* is set
                    to ``some_catalog``, the SQL statement
                    ``SELECT * FROM some_schema.some_table`` will actually
                    query the table
                    ``some_catalog.some_schema.some_table``.
    :param schema: to query. The *schema* is a logical abstraction to group
                   table. This variable sets the default schema used by
                   SQL statements. For example, if *schema* is set to
                   ``some_schema``, the SQL statement
                   ``SELECT * FROM some_table`` will actually query the
                   table ``some_catalog.some_schema.some_table``.
    :param properties: set specific Trino behavior for the current
                               session. Please refer to the output of
                               ``SHOW SESSION`` to check the available
                               properties.
    :param headers: HTTP headers to POST/GET in the HTTP requests
    :param extra_credential: extra credentials. as list of ``(key, value)``
                             tuples.
    :param client_tags: Client tags as list of strings.
    :param roles: roles for the current session. Some connectors do not
                 support role management. See connector documentation for more details.
    :param timezone: The timezone for query processing. Defaults to the system's local timezone.
    :param encoding: The encoding for the spooling protocol. Defaults to None.
    :param heartbeat_interval: interval in seconds between heartbeat requests sent to the coordinator while the
        client is busy downloading spooled segments or slowly consuming results, so that the query is not abandoned
        (``query.client.timeout``). ``None`` disables the heartbeats.
    """

    def __init__(
        self,
        user: str,
        authorization_user: Optional[str] = None,
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
        source: Optional[str] = None,
        properties: Optional[dict[str, str]] = None,
        headers: Optional[dict[str, str]] = None,
        transaction_id: Optional[str] = None,
        extra_credential: Optional[list[tuple[str, str]]] = None,
        client_tags: Optional[list[str]] = None,
        roles: Optional[Union[dict[str, str], str]] = None,
        timezone: Optional[str] = None,
        encoding: Optional[Union[str, list[str]]] = None,
        heartbeat_interval: Optional[float] = constants.DEFAULT_HEARTBEAT_INTERVAL,
    ):
        self._object_lock = threading.Lock()
        self._prepared_statements: dict[str, str] = {}

        self._user = user
        self._authorization_user = authorization_user
        self._catalog = catalog
        self._schema = schema
        self._source = source
        self._properties = properties.copy() if properties is not None else {}
        self._headers = headers.copy() if headers is not None else {}
        self._transaction_id = transaction_id
        self._extra_credential = extra_credential
        self._client_tags = client_tags.copy() if client_tags is not None else []
        self._roles = self._format_roles(roles) if roles is not None else {}
        self._timezone = timezone or get_localzone_name()
        if timezone:  # Check timezone validity
            ZoneInfo(timezone)
        self._encoding = encoding
        self._heartbeat_interval = heartbeat_interval

    @property
    def user(self) -> str:
        return self._user

    @property
    def authorization_user(self) -> Optional[str]:
        with self._object_lock:
            return self._authorization_user

    @authorization_user.setter
    def authorization_user(self, authorization_user: Optional[str]) -> None:
        with self._object_lock:
            self._authorization_user = authorization_user

    @property
    def catalog(self) -> Optional[str]:
        with self._object_lock:
            return self._catalog

    @catalog.setter
    def catalog(self, catalog: Optional[str]) -> None:
        with self._object_lock:
            self._catalog = catalog

    @property
    def schema(self) -> Optional[str]:
        with self._object_lock:
            return self._schema

    @schema.setter
    def schema(self, schema: Optional[str]) -> None:
        with self._object_lock:
            self._schema = schema

    @property
    def source(self) -> Optional[str]:
        return self._source

    @property
    def properties(self) -> dict[str, str]:
        with self._object_lock:
            return self._properties

    @properties.setter
    def properties(self, properties: dict[str, str]) -> None:
        with self._object_lock:
            self._properties = properties

    @property
    def headers(self) -> dict[str, str]:
        return self._headers

    @property
    def transaction_id(self) -> Optional[str]:
        with self._object_lock:
            return self._transaction_id

    @transaction_id.setter
    def transaction_id(self, transaction_id: Optional[str]) -> None:
        with self._object_lock:
            self._transaction_id = transaction_id

    @property
    def extra_credential(self) -> Optional[list[tuple[str, str]]]:
        return self._extra_credential

    @property
    def client_tags(self) -> list[str]:
        return self._client_tags

    @property
    def roles(self) -> dict[str, str]:
        with self._object_lock:
            return self._roles

    @roles.setter
    def roles(self, roles: dict[str, str]) -> None:
        with self._object_lock:
            self._roles = roles

    @property
    def prepared_statements(self) -> dict[str, str]:
        return self._prepared_statements

    @prepared_statements.setter
    def prepared_statements(self, prepared_statements: dict[str, str]) -> None:
        with self._object_lock:
            self._prepared_statements = prepared_statements

    @property
    def timezone(self) -> str:
        with self._object_lock:
            return self._timezone

    @property
    def encoding(self) -> Union[str, list[str]]:
        with self._object_lock:
            return self._encoding

    @property
    def heartbeat_interval(self) -> Optional[float]:
        return self._heartbeat_interval

    @staticmethod
    def _format_roles(roles: Union[dict[str, str], str]) -> dict[str, str]:
        if isinstance(roles, str):
            roles = {"system": roles}
        formatted_roles = {}
        for catalog, role in roles.items():
            is_legacy_role_pattern = ROLE_PATTERN.match(role) is not None
            if role in ("NONE", "ALL") or is_legacy_role_pattern:
                if is_legacy_role_pattern:
                    warnings.warn(
                        f"A role '{role}' is provided using a legacy format. "
                        "Please remove the ROLE{} wrapping. Support for the legacy format might be "
                        "removed in a future release.",
                        DeprecationWarning,
                    )
                formatted_roles[catalog] = role
            else:
                formatted_roles[catalog] = f"ROLE{{{role}}}"
        return formatted_roles

    def __getstate__(self):
        state = self.__dict__.copy()
        del state["_object_lock"]
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self._object_lock = threading.Lock()
        self.__dict__.setdefault("_heartbeat_interval", constants.DEFAULT_HEARTBEAT_INTERVAL)


def get_header_values(headers: CIMultiDict[str], header: str) -> list[str]:
    return [val.strip() for val in headers[header].split(",")]


def get_session_property_values(headers: CIMultiDict[str], header: str) -> list[tuple[str, str]]:
    kvs = get_header_values(headers, header)
    return [(k.strip(), urllib.parse.unquote_plus(v.strip())) for k, v in (kv.split("=", 1) for kv in kvs if kv)]


def get_prepared_statement_values(headers: CIMultiDict[str], header: str) -> list[tuple[str, str]]:
    kvs = get_header_values(headers, header)
    return [(k.strip(), urllib.parse.unquote_plus(v.strip())) for k, v in (kv.split("=", 1) for kv in kvs if kv)]


def get_roles_values(headers: CIMultiDict[str], header: str) -> list[tuple[str, str]]:
    kvs = get_header_values(headers, header)
    return [(k.strip(), urllib.parse.unquote_plus(v.strip())) for k, v in (kv.split("=", 1) for kv in kvs if kv)]


def _ssl_option(verify: Union[bool, str, ssl.SSLContext]) -> Union[bool, ssl.SSLContext]:
    """Translate a requests-style ``verify`` value (bool, CA bundle path or SSLContext) to aiohttp's ``ssl``."""
    if isinstance(verify, bool) or isinstance(verify, ssl.SSLContext):
        return verify
    if isinstance(verify, (str, os.PathLike)):
        return ssl.create_default_context(cafile=os.fspath(verify))
    raise ValueError(f"Invalid value for verify: {verify!r}")


def create_http_session(verify: Union[bool, str, ssl.SSLContext] = True, http_module=aiohttp) -> aiohttp.ClientSession:
    """Create an aiohttp session suitable for talking to a Trino coordinator.

    Trino may send response headers larger than aiohttp's default 8 KiB limit (e.g. ``X-Trino-Set-Session`` or
    ``X-Trino-Added-Prepare`` with large values, see https://github.com/trinodb/trino/issues/8797), so the limits
    are raised to 1 MiB. Must be called with a running event loop.
    """
    return http_module.ClientSession(
        connector=http_module.TCPConnector(ssl=_ssl_option(verify)),
        max_line_size=constants.MAX_RESPONSE_HEADER_SIZE,
        max_field_size=constants.MAX_RESPONSE_HEADER_SIZE,
    )


# Deprecated: the connector used to override aiohttp private APIs to raise the response header size limit.
# create_http_session() configures this through public aiohttp options instead.
TrinoTCPConnector = aiohttp.TCPConnector


@dataclass
class TrinoStatus:
    id: str
    stats: dict[str, str]
    warnings: list[Any]
    info_uri: str
    next_uri: Optional[str]
    update_type: Optional[str]
    update_count: Optional[int]
    rows: Union[list[Any], dict[str, Any]]
    columns: list[Any]

    def __repr__(self):
        return "TrinoStatus(id={}, stats={{...}}, warnings={}, info_uri={}, next_uri={}, rows=<count={}>)".format(
            self.id,
            len(self.warnings),
            self.info_uri,
            self.next_uri,
            len(self.rows),
        )


class _DelayExponential:
    def __init__(
        self,
        base=0.1,
        exponent=2,
        jitter=True,
        max_delay=1800,  # 100ms  # 30 min
    ):
        self._base = base
        self._exponent = exponent
        self._jitter = jitter
        self._max_delay = max_delay

    def __call__(self, attempt):
        delay = float(self._base) * (self._exponent**attempt)
        if self._jitter:
            delay *= random.random()
        delay = min(float(self._max_delay), delay)
        return delay


class _RetryWithExponentialBackoff:
    def __init__(
        self,
        base=0.1,
        exponent=2,
        jitter=True,
        max_delay=1800,  # 100ms  # 30 min
    ):
        self._get_delay = _DelayExponential(base, exponent, jitter, max_delay)

    async def retry(self, func, args, kwargs, err, attempt):
        delay = self._get_delay(attempt)
        await sleep(delay)


class _RetryAfterSleep:
    def __init__(self, retry_after_header):
        self._retry_after_header = retry_after_header

    async def retry(self):
        await sleep(self._retry_after_header)


class TrinoRequest:
    """
    Manage the HTTP requests of a Trino query.

    :param host: name of the coordinator
    :param port: TCP port to connect to the coordinator
    :param http_scheme: "http" or "https"
    :param auth: class that manages user authentication. ``None`` means no
                 authentication.
    :max_attempts: maximum number of attempts when sending HTTP requests. An
                   attempt is an HTTP request. 5 attempts means 4 retries.
    :request_timeout: How long (in seconds) to wait for the server to send
                      data before giving up, as a float or a
                      ``(connect timeout, read timeout)`` tuple.

    The client initiates a query by sending an HTTP POST to the
    coordinator. It then gets a response back from the coordinator with:
    - An URI to query to get the status for the query and the remaining
      data
    - An URI to get more information about the execution of the query
    - Statistics about the current query execution

    Please refer to :class:`TrinoStatus` to access the status returned by
    :meth:`TrinoRequest.process`.

    When the client makes an HTTP request, it may encounter the following
    errors:
    - Connection or read timeout:
      - There is a network partition and TCP segments are
        either dropped or delayed.
      - The coordinator stalled because of an OS level stall (page allocation
        stall, long time to page in pages, etc...), a JVM stall (full GC), or
        an application level stall (thread starving, lock contention)
    - Connection refused: Configuration or runtime issue on the coordinator
    - Connection closed:

    As most of these errors are transient, the question the caller should set
    retries with respect to when they want to notify the application that uses
    the client.
    """

    http = aiohttp

    # Errors after which a non idempotent request (the statement POST) is retried
    HTTP_EXCEPTIONS = (
        aiohttp.ClientConnectorError,  # the connection could not be established, nothing was sent
        aiohttp.ServerConnectionError,  # type: ignore
        aiohttp.ServerTimeoutError,  # type: ignore
    )
    # Errors after which an idempotent request (GET nextUri, segment download, HEAD, DELETE) is retried, including
    # a connection reset while reading the response body.
    IDEMPOTENT_HTTP_EXCEPTIONS = (
        aiohttp.ClientConnectionError,
        aiohttp.ClientPayloadError,
        asyncio.TimeoutError,
    )

    def __init__(
        self,
        host: str,
        port: int,
        client_session: ClientSession,
        http_session: Optional[aiohttp.ClientSession] = None,
        http_scheme: Optional[str] = None,
        auth: Optional[Authentication] = constants.DEFAULT_AUTH,
        max_attempts: int = MAX_ATTEMPTS,
        request_timeout: Union[float, tuple[float, float]] = constants.DEFAULT_REQUEST_TIMEOUT,
        handle_retry=_RetryWithExponentialBackoff(),
        verify: bool = True,
        segment_http_session_provider: Optional[Callable[[], aiohttp.ClientSession]] = None,
    ):
        """
        :param segment_http_session_provider: returns the session used to download and acknowledge spooled
            segments. It must not carry credentials nor default headers: segments are usually downloaded from
            an object storage using pre-signed URLs. When omitted, the request creates (and closes) its own.
        """
        self._client_session = client_session

        self._host = host
        self._port = port
        self._next_uri: Optional[str] = None

        if http_scheme is None:
            if self._port == constants.DEFAULT_TLS_PORT:
                self._http_scheme = constants.HTTPS
            else:
                self._http_scheme = constants.HTTP
        else:
            self._http_scheme = http_scheme

        self._verify = verify
        self._segment_http_session_provider = segment_http_session_provider
        self._own_segment_http_session: Optional[aiohttp.ClientSession] = None
        self._unauthenticated_request: Optional[TrinoRequest] = None
        if http_session is not None:
            self._http_session = http_session
            self._close_session = False
        else:
            self._http_session = create_http_session(verify, http_module=self.http)
            self._close_session = True
        # Trino headers are computed for every request from the current client session state (they change
        # with SET SESSION, USE, PREPARE, ...), they are not stored as defaults of the (shared) aiohttp session.
        # Compute them once to validate the custom headers and the extra credentials early.
        _ = self.http_headers
        self._exceptions = self.HTTP_EXCEPTIONS
        self._idempotent_exceptions = self.IDEMPOTENT_HTTP_EXCEPTIONS
        self._auth = auth
        if self._auth:
            self._auth.set_http_session(self._http_session)
            self._exceptions += self._auth.get_exceptions()
            self._idempotent_exceptions += self._auth.get_exceptions()

        self._request_timeout = request_timeout
        self._handle_retry = handle_retry
        self.max_attempts = max_attempts
        # monotonic time of the last request to the coordinator for the current query (statement or heartbeat)
        self.last_request_time = time.monotonic()

    def clone(self) -> "TrinoRequest":
        """Because copy.deepcopy doesn't work with Futures"""
        return TrinoRequest(
            host=self._host,
            port=self._port,
            client_session=self._client_session,
            http_session=self._http_session,
            http_scheme=self._http_scheme,
            auth=self._auth,
            max_attempts=self.max_attempts,
            request_timeout=self._request_timeout,
            handle_retry=self._handle_retry,
            verify=self._verify,
            segment_http_session_provider=self._segment_http_session_provider,
        )

    async def __aenter__(self):
        return self

    async def __aexit__(self, *excinfo):
        await self.close()

    async def _close_http_session(self):
        if self._http_session is not None and self._close_session:
            if not self._http_session.closed:
                await self._http_session.close()
            self._http_session = None
        if self._own_segment_http_session is not None:
            await self._own_segment_http_session.close()
            self._own_segment_http_session = None

    async def close(self):
        await self._close_http_session()

    @property
    def transaction_id(self) -> Optional[str]:
        return self._client_session.transaction_id

    @transaction_id.setter
    def transaction_id(self, value: Optional[str]) -> None:
        self._client_session.transaction_id = value

    @property
    def http_headers(self) -> CIMultiDict[str]:
        headers: CIMultiDict[str] = CIMultiDict()

        headers[constants.HEADER_CATALOG] = self._client_session.catalog
        headers[constants.HEADER_SCHEMA] = self._client_session.schema
        headers[constants.HEADER_SOURCE] = self._client_session.source
        if self._client_session.authorization_user is not None:
            # SET SESSION AUTHORIZATION / impersonation: the original user authenticates, the queries run as the
            # authorization user
            headers[constants.HEADER_ORIGINAL_USER] = self._client_session.user
            headers[constants.HEADER_USER] = self._client_session.authorization_user
        else:
            headers[constants.HEADER_USER] = self._client_session.user
        headers[constants.HEADER_TIMEZONE] = self._client_session.timezone
        if self._client_session.encoding is None:
            pass
        elif isinstance(self._client_session.encoding, list):
            headers[constants.HEADER_ENCODING] = ",".join(self._client_session.encoding)
        elif isinstance(self._client_session.encoding, str):
            headers[constants.HEADER_ENCODING] = self._client_session.encoding
        else:
            raise ValueError("Invalid type for encoding: expected str or list")
        headers[constants.HEADER_CLIENT_CAPABILITIES] = constants.CLIENT_CAPABILITIES
        headers["user-agent"] = f"{constants.CLIENT_NAME}/{__version__}"
        if len(self._client_session.roles.values()):
            headers[constants.HEADER_ROLE] = ",".join(
                # ``name`` must not contain ``=``
                "{}={}".format(catalog, urllib.parse.quote(str(role)))
                for catalog, role in self._client_session.roles.items()
            )
        if self._client_session.client_tags is not None and len(self._client_session.client_tags) > 0:
            headers[constants.HEADER_CLIENT_TAGS] = ",".join(self._client_session.client_tags)

        headers[constants.HEADER_SESSION] = ",".join(
            # ``name`` must not contain ``=``
            "{}={}".format(name, urllib.parse.quote(str(value)))
            for name, value in self._client_session.properties.items()
        )

        if len(self._client_session.prepared_statements) != 0:
            # ``name`` must not contain ``=``
            headers[constants.HEADER_PREPARED_STATEMENT] = ",".join(
                "{}={}".format(name, urllib.parse.quote_plus(statement))
                for name, statement in self._client_session.prepared_statements.items()
            )

        # merge custom http headers
        for key in self._client_session.headers:
            if key in headers.keys():
                raise ValueError("cannot override reserved HTTP header {}".format(key))
        headers.update(self._client_session.headers)

        transaction_id = self._client_session.transaction_id
        headers[constants.HEADER_TRANSACTION] = transaction_id

        if self._client_session.extra_credential is not None and len(self._client_session.extra_credential) > 0:
            for tup in self._client_session.extra_credential:
                self._verify_extra_credential(tup)

            # HTTP 1.1 section 4.2 combine multiple extra credentials into a
            # comma-separated value
            # extra credential value is encoded per spec (application/x-www-form-urlencoded MIME format)
            headers[constants.HEADER_EXTRA_CREDENTIAL] = ", ".join(
                [f"{tup[0]}={urllib.parse.quote_plus(str(tup[1]))}" for tup in self._client_session.extra_credential]
            )

        # Remove None values as aiohttp doesn't handle them
        return CIMultiDict({k: v for k, v in headers.items() if v is not None})

    def _segment_http_session(self) -> aiohttp.ClientSession:
        if self._segment_http_session_provider is not None:
            return self._segment_http_session_provider()
        if self._own_segment_http_session is None or self._own_segment_http_session.closed:
            self._own_segment_http_session = create_http_session(self._verify, http_module=self.http)
        return self._own_segment_http_session

    def unauthenticated(self) -> TrinoRequest:
        """Return a request to download and acknowledge spooled segments.

        It uses a dedicated HTTP session without authentication nor default headers: spooled segments are
        usually served by an object storage through pre-signed URLs, which reject (or must never see) the
        credentials and the session headers sent to the coordinator.
        """
        if self._unauthenticated_request is None:
            self._unauthenticated_request = TrinoRequest(
                host=self._host,
                port=self._port,
                client_session=ClientSession(user=self._client_session.user, timezone=self._client_session.timezone),
                http_session=self._segment_http_session(),
                http_scheme=self._http_scheme,
                auth=None,
                max_attempts=self.max_attempts,
                request_timeout=self._request_timeout,
                handle_retry=self._handle_retry,
                verify=self._verify,
            )
        return self._unauthenticated_request

    @property
    def max_attempts(self) -> int:
        return self._max_attempts

    @max_attempts.setter
    def max_attempts(self, value: int) -> None:
        self._max_attempts = value
        # The response body is read within the retry loop, so a connection reset while reading it is retried, and
        # the responses which trigger a retry (429, 502, 503, 504) are released.
        retry_on_status = (
            # need retry when there is no exception but the status code is 429, 502, 503, or 504
            lambda response: getattr(response, "status", None) in (429, 502, 503, 504),
        )

        def with_retry(handled_exceptions, retry_empty_body=False):
            return _retry_with(
                self._handle_retry,
                handled_exceptions=handled_exceptions,
                conditions=retry_on_status,
                max_attempts=value,
                read_body=True,
                retry_empty_body=retry_empty_body,
            )

        self._get = with_retry(self._idempotent_exceptions)(self._http_session.get)
        self._post = with_retry(self._exceptions)(self._http_session.post)
        self._delete = with_retry(self._idempotent_exceptions)(self._http_session.delete)
        self._head = with_retry(self._idempotent_exceptions)(self._http_session.head)
        # The coordinator may answer a statement request with an empty HTTP 200 under load (upstream #596), these
        # are retried. Not for segment downloads and acknowledgements (upstream #636).
        self._get_statement = with_retry(self._idempotent_exceptions, retry_empty_body=True)(self._http_session.get)
        self._post_statement = with_retry(self._exceptions, retry_empty_body=True)(self._http_session.post)

    def get_url(self, path: str) -> str:
        return "{protocol}://{host}:{port}{path}".format(protocol=self._http_scheme, host=self._host, port=self._port, path=path)

    @property
    def statement_url(self) -> str:
        return self.get_url(constants.URL_STATEMENT_PATH)

    @property
    def next_uri(self) -> Optional[str]:
        return self._next_uri

    async def post(self, sql: str, additional_http_headers: Optional[dict[str, Any]] = None) -> aiohttp.ClientResponse:
        data = sql.encode("utf-8")
        # Deep copy of the http_headers dict since they may be modified for this
        # request by the provided additional_http_headers
        http_headers = copy.deepcopy(self.http_headers)

        # Update the request headers with the additional_http_headers
        http_headers.update(additional_http_headers or {})

        # The Trino protocol expects UTF-8 encoded SQL text. Send the charset explicitly like the JDBC driver so that
        # proxies such as Trino Gateway can parse the statement (upstream #600). Users may still override it.
        http_headers.setdefault(constants.HEADER_CONTENT_TYPE, constants.CONTENT_TYPE_TEXT_UTF8)

        self.last_request_time = time.monotonic()
        http_response = await self._post_statement(
            self.statement_url,
            data=data,
            headers=http_headers,
            timeout=self.request_client_timeout,
            # TODO: proxies=PROXIES,
        )
        return http_response

    async def get(self, url: str) -> aiohttp.ClientResponse:
        self.last_request_time = time.monotonic()
        return await self._get_statement(
            url,
            headers=self.http_headers,
            timeout=self.request_client_timeout,
            # TODO: proxies=PROXIES,
        )

    async def delete(self, url: str) -> aiohttp.ClientResponse:
        return await self._delete(
            url,
            headers=self.http_headers,
            timeout=self.request_client_timeout,
            # TODO: proxies=PROXIES,
        )

    async def head(self, url: str) -> aiohttp.ClientResponse:
        self.last_request_time = time.monotonic()
        return await self._head(
            url,
            headers=self.http_headers,
            timeout=self.request_client_timeout,
        )

    @property
    def request_client_timeout(self) -> aiohttp.ClientTimeout:
        """Timeout of the requests to the coordinator."""
        if isinstance(self._request_timeout, tuple):
            connect, read = self._request_timeout
            return aiohttp.ClientTimeout(sock_connect=connect, sock_read=read)
        return aiohttp.ClientTimeout(total=self._request_timeout)

    @property
    def segment_client_timeout(self) -> aiohttp.ClientTimeout:
        """Timeout of spooled segment downloads: segments may be large, so there is no limit on the total duration,
        only on connecting and on waiting for data."""
        if isinstance(self._request_timeout, tuple):
            connect, read = self._request_timeout
        else:
            connect = read = self._request_timeout
        return aiohttp.ClientTimeout(total=None, sock_connect=connect, sock_read=read)

    @staticmethod
    def _process_error(error, query_id: Optional[str]) -> Union[TrinoExternalError, TrinoQueryError, TrinoUserError]:
        error_type = error.get("errorType")
        if error_type == "EXTERNAL":
            raise exceptions.TrinoExternalError(error, query_id)
        elif error_type == "USER_ERROR":
            return exceptions.TrinoUserError(error, query_id)
        elif error_type == "INTERNAL_ERROR":
            return exceptions.TrinoInternalError(error, query_id)

        return exceptions.TrinoQueryError(error, query_id)

    @staticmethod
    async def raise_response_error(http_response: aiohttp.ClientResponse) -> None:
        if http_response.status == 502:
            raise exceptions.Http502Error("error 502: bad gateway")

        if http_response.status == 503:
            raise exceptions.Http503Error("error 503: service unavailable")

        if http_response.status == 504:
            raise exceptions.Http504Error("error 504: gateway timeout")

        raise exceptions.HttpError(
            "error {}{}".format(
                http_response.status,
                ": {}".format(await http_response.read()),
            )
        )

    async def process(self, http_response: aiohttp.ClientResponse) -> TrinoStatus:
        # type: (aiohttp.ClientResponse) -> TrinoStatus
        if not http_response.ok:
            await self.raise_response_error(http_response)

        try:
            # the body is already read (see _retry_with), content_type=None: proxies may not set application/json
            response: dict[str, Any] = await http_response.json(encoding="utf-8", loads=_json_loads, content_type=None)
        except ValueError as e:
            raise exceptions.TrinoConnectionError(f"failed to parse the response of the server: {e}") from e
        if response is None:
            raise exceptions.TrinoConnectionError(f"received empty response from server (status {http_response.status})")
        logger.debug("HTTP %s: %s", http_response.status, response)
        if "error" in response and response["error"]:
            raise self._process_error(response["error"], response.get("id"))

        if constants.HEADER_CLEAR_SESSION in http_response.headers:
            for prop in get_header_values(http_response.headers, constants.HEADER_CLEAR_SESSION):
                self._client_session.properties.pop(prop, None)

        if constants.HEADER_SET_SESSION in http_response.headers:
            for key, value in get_session_property_values(http_response.headers, constants.HEADER_SET_SESSION):
                self._client_session.properties[key] = value

        if constants.HEADER_SET_CATALOG in http_response.headers:
            self._client_session.catalog = http_response.headers[constants.HEADER_SET_CATALOG]

        if constants.HEADER_SET_SCHEMA in http_response.headers:
            self._client_session.schema = http_response.headers[constants.HEADER_SET_SCHEMA]

        if constants.HEADER_SET_ROLE in http_response.headers:
            for key, value in get_roles_values(http_response.headers, constants.HEADER_SET_ROLE):
                self._client_session.roles[key] = value

        if constants.HEADER_ADDED_PREPARE in http_response.headers:
            for name, statement in get_prepared_statement_values(http_response.headers, constants.HEADER_ADDED_PREPARE):
                self._client_session.prepared_statements[name] = statement

        if constants.HEADER_DEALLOCATED_PREPARE in http_response.headers:
            for name in get_header_values(http_response.headers, constants.HEADER_DEALLOCATED_PREPARE):
                self._client_session.prepared_statements.pop(name, None)

        if constants.HEADER_SET_AUTHORIZATION_USER in http_response.headers:
            self._client_session.authorization_user = http_response.headers[constants.HEADER_SET_AUTHORIZATION_USER]

        if constants.HEADER_RESET_AUTHORIZATION_USER in http_response.headers:
            self._client_session.authorization_user = None

        self._next_uri = response.get("nextUri")

        return TrinoStatus(
            id=response["id"],
            stats=response["stats"],
            warnings=response.get("warnings", []),
            info_uri=response["infoUri"],
            next_uri=self._next_uri,
            update_type=response.get("updateType"),
            update_count=response.get("updateCount"),
            rows=response.get("data") if response.get("data") else [],
            columns=response.get("columns"),
        )

    @staticmethod
    def _verify_extra_credential(header: tuple[str, str]) -> None:
        """
        Verifies that key has ASCII only and non-whitespace characters.
        """
        key = header[0]

        if not _HEADER_EXTRA_CREDENTIAL_KEY_REGEX.match(key):
            raise ValueError(f"whitespace or '=' are disallowed in extra credential '{key}'")

        try:
            key.encode().decode("ascii")
        except UnicodeDecodeError:
            raise ValueError(f"only ASCII characters are allowed in extra credential '{key}'") from None


_YIELD_EVERY_ROWS = 1000


class TrinoResult:
    """
    Represent the result of a Trino query as an async iterator on rows.

    The iteration state is kept on the instance instead of in an async generator: a generator which raises is
    finalized and every later ``__anext__`` raises ``StopAsyncIteration``, indistinguishable from the normal end of
    the result. Keeping the state here lets a transient error (e.g. a failed spooled segment download) propagate to
    the caller while a retried ``__anext__`` resumes where the failure happened, instead of silently truncating the
    result (upstream trino-python-client #598).

    A batch of rows is either a list (direct protocol, segment fetch mode) or an async iterator which loads the
    spooled segments lazily (:class:`SegmentIterator`).
    """

    def __init__(self, query, rows: Union[list[Any], AsyncIterator[Any]]):
        self._query = query
        # Initial rows from the first POST request
        self._rows = rows
        self._rownumber = 0
        # Iterator over the batch of rows currently being served
        self._current_batch: Optional[Union[Iterator[Any], AsyncIterator[Any]]] = None
        # Rows of the next page, fetched before serving the current batch
        self._next_rows: Optional[Union[list[Any], AsyncIterator[Any]]] = None

    @property
    def rows(self):
        return self._rows

    @rows.setter
    def rows(self, rows):
        self._rows = rows

    @property
    def rownumber(self) -> int:
        return self._rownumber

    def __aiter__(self):
        return self

    async def __anext__(self) -> Any:
        while True:
            batch = self._current_batch
            if batch is None:
                if self._query.finished and self._rows is None:
                    raise StopAsyncIteration
                # A query only transitions to a FINISHED state when the results are fully consumed:
                # The reception of the data is acknowledged by calling the next_uri before exposing the data through
                # dbapi. If fetch() fails, the state is unchanged and the next call fetches the same page again.
                self._next_rows = await self._query.fetch() if not self._query.finished else None
                batch = self._current_batch = _iterate(self._rows)

            try:
                if isinstance(batch, Iterator):
                    row = next(batch)
                else:
                    row = await batch.__anext__()
            except (StopIteration, StopAsyncIteration):
                self._rows = self._next_rows
                self._next_rows = None
                self._current_batch = None
                continue
            self._rownumber += 1
            self._query.heartbeat()
            if self._rownumber % _YIELD_EVERY_ROWS == 0:
                # iterating over rows already in memory never suspends: let other tasks run from time to time
                await asyncio.sleep(0)
            return row

    async def aclose(self) -> None:
        """Release the resources (pending segment downloads) of the batches not fully consumed."""
        for rows in (self._current_batch, self._rows, self._next_rows):
            await _aclose_rows(rows)


def _iterate(rows: Union[Iterable[Any], AsyncIterator[Any]]) -> Union[Iterator[Any], AsyncIterator[Any]]:
    if hasattr(rows, "__anext__"):
        return rows
    return iter(rows)


async def _aclose_rows(rows: Any) -> None:
    aclose = getattr(rows, "aclose", None)
    if aclose is not None:
        await aclose()


class _ChainedRows:
    """Async iterator over the rows of several batches (lists or async iterators), loaded lazily."""

    def __init__(self, *parts: Union[Iterable[Any], AsyncIterator[Any]]) -> None:
        self._parts = list(parts)
        self._current: Optional[Union[Iterator[Any], AsyncIterator[Any]]] = None

    def __aiter__(self):
        return self

    async def __anext__(self) -> Any:
        while True:
            if self._current is None:
                if not self._parts:
                    raise StopAsyncIteration
                self._current = _iterate(self._parts.pop(0))
            try:
                if isinstance(self._current, Iterator):
                    return next(self._current)
                return await self._current.__anext__()
            except (StopIteration, StopAsyncIteration):
                self._current = None

    async def aclose(self) -> None:
        for rows in (self._current, *self._parts):
            await _aclose_rows(rows)


_NO_ROW = object()
_HEARTBEAT_MAX_FAILURES = 3


async def _first_row(rows: AsyncIterator[Any]) -> Any:
    try:
        return await rows.__anext__()
    except StopAsyncIteration:
        return _NO_ROW


class TrinoQuery(object):
    """Represent the execution of a SQL statement by Trino."""

    def __init__(
        self,
        request: TrinoRequest,
        query: str,
        legacy_primitive_types: bool = False,
        fetch_mode: Literal["mapped", "segments"] = "mapped",
    ) -> None:
        self._query_id: Optional[str] = None
        self._stats: dict[Any, Any] = {}
        self._info_uri: Optional[str] = None
        self._warnings: list[dict[Any, Any]] = []
        self._columns: Optional[list[str]] = None
        self._finished = False
        self._cancelled = False
        self._request = request
        self._update_type = None
        self._update_count = None
        self._next_uri = None
        self._query = query
        self._result: Optional[TrinoResult] = None
        self._legacy_primitive_types = legacy_primitive_types
        self._row_mapper: Optional[RowMapper] = None
        self._fetch_mode = fetch_mode
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._heartbeat_failures = 0
        self._heartbeat_disabled = False

    @property
    def query_id(self) -> Optional[str]:
        return self._query_id

    @property
    def query(self) -> Optional[str]:
        return self._query

    async def get_columns(self):
        if self.query_id:
            while not self._columns and not self.finished and not self.cancelled:
                # Columns are not returned immediately after query is submitted.
                # Continue fetching data until columns information is available and push fetched rows into buffer.
                self._append_rows(await self.fetch())
        return self._columns

    def _append_rows(self, new_rows: Union[list[Any], AsyncIterator[Any]]) -> None:
        rows = self._result.rows
        if isinstance(rows, list) and isinstance(new_rows, list):
            rows += new_rows
        elif isinstance(rows, list) and not rows:
            self._result.rows = new_rows
        else:
            # spooled segments are loaded lazily, do not download them to concatenate them
            self._result.rows = _ChainedRows(rows, new_rows)

    @property
    def columns(self):
        """The columns of the result, ``None`` until received from the coordinator (see :meth:`get_columns`)."""
        return self._columns

    @property
    def stats(self):
        return self._stats

    @property
    def update_type(self):
        return self._update_type

    @property
    def update_count(self):
        return self._update_count

    @property
    def warnings(self):
        return self._warnings

    @property
    def result(self):
        return self._result

    @property
    def info_uri(self):
        return self._info_uri

    async def execute(self, additional_http_headers=None) -> TrinoResult:
        """Initiate a Trino query by sending the SQL statement

        This is the first HTTP request sent to the coordinator.
        It sets the query_id and returns a Result object used to
        track the rows returned by the query. To fetch all rows,
        call fetch() until is_finished is true.
        """
        if self._cancelled:
            raise exceptions.TrinoUserError("Query has been cancelled", self.query_id)

        try:
            response = await self._request.post(self._query, additional_http_headers)
        except (aiohttp.client_exceptions.ClientError, asyncio.TimeoutError) as e:
            raise exceptions.TrinoConnectionError("failed to execute: {}".format(e)) from None
        status = await self._request.process(response)
        self._info_uri = status.info_uri
        self._query_id = status.id
        self._stats.update({"queryId": self.query_id})
        self._update_state(status)
        self._warnings = getattr(status, "warnings", [])
        if status.next_uri is None:
            self._finished = True

        self._result = TrinoResult(self, self._rows_from_status(status) if self._row_mapper else status.rows)

        # Block until rows are available, the query finishes, or it is cancelled.
        # Early responses often contain only stats, so keep fetching until actual data arrives.
        # The direct protocol returns lists of rows, the spooling protocol a lazy iterator on the rows of the
        # segments: its first row is loaded to know whether it is empty (the other segments stay lazy).
        while not self.finished and not self.cancelled and self._result.rows == []:
            new_rows = await self.fetch()
            if isinstance(new_rows, list):
                self._result.rows += new_rows
            else:
                first_row = await _first_row(new_rows)
                if first_row is not _NO_ROW:
                    self._result.rows = _ChainedRows([first_row], new_rows)

        # Update statements (INSERT/UPDATE/DELETE/DDL/...) report their affected row count as a single row, but Trino
        # still returns a final nextUri that must be consumed for the query to reach a terminal state. Unlike a SELECT
        # there is no result set to stream, so drain the remaining pages now. Otherwise closing the cursor without
        # fetching issues a DELETE against an already completed statement, which Trino reports as USER_CANCELED
        # (upstream trino-python-client #601).
        while self._update_type is not None and not self.finished and not self.cancelled:
            self._append_rows(await self.fetch())
        return self._result

    def _update_state(self, status: TrinoStatus) -> None:
        self._stats.update(status.stats)
        self._update_type = status.update_type
        self._update_count = status.update_count
        self._next_uri = status.next_uri
        if not self._row_mapper and status.columns:
            self._row_mapper = RowMapperFactory().create(
                columns=status.columns, legacy_primitive_types=self._legacy_primitive_types
            )
        if status.columns:
            self._columns = status.columns

    async def fetch(self) -> Union[list[Any], AsyncIterator[list[Any]]]:
        """Continue fetching data for the current query_id.

        Returns a list of rows (direct protocol), a list of :class:`DecodableSegment` (``segments`` fetch mode) or
        an async iterator which downloads and decodes the spooled segments lazily (spooling protocol).
        """
        try:
            response = await self._request.get(self._request.next_uri)
        except (aiohttp.client_exceptions.ClientError, asyncio.TimeoutError) as e:
            raise exceptions.TrinoConnectionError("failed to fetch: {}".format(e)) from None
        status = await self._request.process(response)
        self._update_state(status)
        if status.next_uri is None:
            self._finished = True

        if not self._row_mapper:
            return []
        return self._rows_from_status(status)

    def _rows_from_status(self, status: TrinoStatus) -> Union[list[Any], AsyncIterator[list[Any]]]:
        rows = status.rows
        if isinstance(rows, dict):
            # spooling protocol
            spooled = self._to_segments(cast(_SpooledProtocolResponseTO, rows))
            if self._fetch_mode == "segments":
                return spooled
            if not spooled:
                return []
            # Do not materialize the rows: segments are downloaded and decoded while iterating
            return SegmentIterator(spooled, self._row_mapper, query=self)
        elif isinstance(rows, list):
            return self._row_mapper.map(rows)
        else:
            raise ValueError(f"Unexpected type: {type(rows)}")

    def _to_segments(self, rows: _SpooledProtocolResponseTO) -> list[DecodableSegment]:
        encoding = rows["encoding"]
        metadata = rows["metadata"] if "metadata" in rows else None
        segments = []
        for segment in rows["segments"]:
            segment_type = segment["type"]
            if segment_type == SegmentType.INLINE:
                inline_segment = cast(_InlineSegmentTO, segment)
                segments.append(InlineSegment(inline_segment))
            elif segment_type == SegmentType.SPOOLED:
                spooled_segment = cast(_SpooledSegmentTO, segment)
                segments.append(
                    SpooledSegment(
                        spooled_segment,
                        self._request.unauthenticated(),
                        coordinator_host=self._request._host,
                        custom_headers=dict(self._request._client_session.headers),
                    )
                )
            else:
                raise ValueError(f"Unsupported segment type: {segment_type}")

        return [DecodableSegment(encoding, metadata, segment) for segment in segments]

    async def cancel(self) -> None:
        """Cancel the current query"""
        if self._next_uri is None:
            return

        logger.debug("cancelling query: %s", self.query_id)
        try:
            response = await self._request.delete(self._next_uri)
        except (aiohttp.client_exceptions.ClientError, asyncio.TimeoutError) as e:
            raise exceptions.TrinoConnectionError("failed to cancel query: {}".format(e)) from None
        if response.status == 204:
            self._cancelled = True
            logger.debug("query cancelled: %s", self.query_id)
            return

        await self._request.raise_response_error(response)

    @property
    def heartbeat_interval(self) -> Optional[float]:
        return self._request._client_session.heartbeat_interval

    def heartbeat(self) -> None:
        """Send a heartbeat (``HEAD nextUri``) in the background if the coordinator did not hear from the client for
        the heartbeat interval, so that it does not abandon the query while the client is busy (e.g. downloading
        spooled segments or consuming rows slowly). Cheap to call often.

        Like the Java client, heartbeats are disabled after 3 consecutive failures or if the server does not support
        them.
        """
        interval = self.heartbeat_interval
        if not interval or self._heartbeat_disabled or self._finished or self._cancelled:
            return
        if self._heartbeat_task is not None and not self._heartbeat_task.done():
            return
        next_uri = self._request.next_uri
        if next_uri is None or time.monotonic() - self._request.last_request_time < interval:
            return
        self._request.last_request_time = time.monotonic()
        self._heartbeat_task = asyncio.get_running_loop().create_task(self._send_heartbeat(next_uri))

    async def _send_heartbeat(self, uri: str) -> None:
        try:
            response = await self._request.head(uri)
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001 - heartbeats are best effort
            logger.debug("heartbeat for query %s failed: %s", self.query_id, e)
            self._heartbeat_failures += 1
        else:
            if response.status in (404, 405):
                logger.warning("The server does not support heartbeat calls")
                self._heartbeat_disabled = True
                return
            if response.status >= 400:
                self._heartbeat_failures += 1
            else:
                self._heartbeat_failures = 0
        if self._heartbeat_failures >= _HEARTBEAT_MAX_FAILURES:
            logger.warning("Stopping the heartbeat after %s consecutive errors", _HEARTBEAT_MAX_FAILURES)
            self._heartbeat_disabled = True

    @contextlib.asynccontextmanager
    async def keep_alive(self) -> AsyncIterator[None]:
        """Send heartbeats in the background while the block runs (e.g. a long segment download)."""
        interval = self.heartbeat_interval
        if not interval:
            yield
            return

        async def loop() -> None:
            while True:
                await asyncio.sleep(interval / 2)
                self.heartbeat()

        task = asyncio.get_running_loop().create_task(loop())
        try:
            yield
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def aclose(self) -> None:
        """Release the resources of the result (pending segment downloads, heartbeats)."""
        if self._result is not None:
            await self._result.aclose()
        if self._heartbeat_task is not None and not self._heartbeat_task.done():
            self._heartbeat_task.cancel()
            await asyncio.gather(self._heartbeat_task, return_exceptions=True)

    def is_finished(self) -> bool:
        import warnings

        warnings.warn("is_finished is deprecated, use finished instead", DeprecationWarning)
        return self.finished

    @property
    def finished(self) -> bool:
        return self._finished

    @property
    def cancelled(self) -> bool:
        return self._cancelled


def _retry_with(handle_retry, handled_exceptions, conditions, max_attempts, *, read_body=False, retry_empty_body=False):
    """Retry an async request function.

    :param handled_exceptions: exceptions after which the request is retried
    :param conditions: predicates on the response which trigger a retry (e.g. HTTP 503)
    :param read_body: read the response body as part of each attempt, so an error while reading the body is retried
        and the connection is released to the pool
    :param retry_empty_body: retry an HTTP 200 response with an empty body (requires ``read_body``)
    """

    def wrapper(func):
        @functools.wraps(func)
        async def decorated(*args, **kwargs):
            attempt = 0
            for attempt in range(1, max_attempts + 1):  # noqa: B007
                last_attempt = attempt == max_attempts
                result = None
                try:
                    result = await func(*args, **kwargs)
                    body = await result.read() if read_body else None
                except Exception as err:
                    if result is not None:
                        _close_response(result)
                    if last_attempt or not any(isinstance(err, exc) for exc in handled_exceptions):
                        logger.info("failed after %s attempts", attempt)
                        raise
                    logger.debug("retrying after error (attempt %s of %s): %r", attempt, max_attempts, err)
                    await handle_retry.retry(func, args, kwargs, err, attempt)
                    continue

                retry_status = any(guard(result) for guard in conditions)
                empty_body = retry_empty_body and getattr(result, "status", None) == 200 and not body
                if last_attempt or not (retry_status or empty_body):
                    return result
                logger.debug("retrying after HTTP %s (attempt %s of %s)", result.status, attempt, max_attempts)
                _release_response(result)
                if result.status == 429 and "Retry-After" in result.headers:
                    retry_after = _parse_retry_after_header(result.headers.get("Retry-After"))
                    await _RetryAfterSleep(retry_after).retry()
                else:
                    await handle_retry.retry(func, args, kwargs, None, attempt)

        return decorated

    return wrapper


def _release_response(response) -> None:
    try:
        response.release()
    except Exception:  # noqa: BLE001 - best effort
        pass


def _close_response(response) -> None:
    try:
        response.close()
    except Exception:  # noqa: BLE001 - best effort
        pass


def _parse_retry_after_header(retry_after):
    if isinstance(retry_after, int):
        return retry_after
    elif isinstance(retry_after, str) and retry_after.isdigit():
        return int(retry_after)
    else:
        retry_date = parsedate_to_datetime(retry_after)
        now = datetime.utcnow()
        return (retry_date - now).total_seconds()


# Trino Spooled protocol transfer objects
class _SpooledProtocolResponseTO(TypedDict):
    encoding: Literal["json", "json+std", "json+lz4"]
    metadata: _SegmentMetadataTO
    segments: list[_SegmentTO]


class _SegmentMetadataTO(TypedDict):
    uncompressedSize: str
    segmentSize: str


class _SegmentTO(_SegmentMetadataTO):
    type: Literal["spooled", "inline"]
    metadata: _SegmentMetadataTO


class _SpooledSegmentTO(_SegmentTO):
    uri: str
    ackUri: str
    headers: dict[str, list[str]]


class _InlineSegmentTO(_SegmentTO):
    data: str


class SegmentType(str, Enum):
    """Enum with string values that can be compared to strings."""

    INLINE = "inline"
    SPOOLED = "spooled"


class Segment(abc.ABC):
    """
    Abstract base class representing a segment of data produced by the spooling protocol.

    Attributes:
        metadata (property): Metadata associated with the segment.
        rows (property): Returns the decoded and mapped data.
    """

    def __init__(self, segment: _SegmentTO) -> None:
        self._segment = segment

    @abstractmethod
    async def get_data(self):
        pass

    @property
    def metadata(self) -> _SegmentMetadataTO:
        return self._segment["metadata"]


class InlineSegment(Segment):
    """
    A subclass of Segment that handles inline data segments. The data is base64 encoded and
    requires mapping to rows using the provided row_mapper.

    Attributes:
        rows (property): The data in the segment, decoded and mapped from the base64 encoded data.
    """

    def __init__(self, segment: _InlineSegmentTO) -> None:
        super().__init__(segment)
        self._segment = cast(_InlineSegmentTO, segment)

    async def get_data(self) -> bytes:
        return base64.b64decode(self._segment["data"])

    def __repr__(self):
        return f"InlineSegment(metadata={self.metadata})"


class SpooledSegment(Segment):
    """
    A subclass of Segment that handles spooled data segments, where data may be compressed and needs to be
    retrieved via HTTP requests. The segment includes methods for acknowledging processing and loading the
    segment from remote storage.

    Attributes:
        rows (property): The data, loaded and mapped from the spooled segment.
        uri (property): The URI for the spooled segment.
        ack_uri (property): The URI for acknowledging the processing of the spooled segment.
        headers (property): The headers associated with the spooled segment.

    Methods:
        acknowledge(): Sends an acknowledgment request for the segment.
    """

    def __init__(
        self,
        segment: _SpooledSegmentTO,
        request: TrinoRequest,
        coordinator_host: Optional[str] = None,
        custom_headers: Optional[dict[str, str]] = None,
    ) -> None:
        """
        :param request: request without credentials, see :meth:`TrinoRequest.unauthenticated`
        :param coordinator_host: host name of the coordinator; the custom HTTP headers of the connection are
            only sent to that host (e.g. for the acknowledgement), never to an external storage
        :param custom_headers: custom HTTP headers of the connection (``http_headers``)
        """
        super().__init__(segment)
        self._segment = cast(_SpooledSegmentTO, segment)
        self._request = request
        self._coordinator_host = coordinator_host
        self._custom_headers = custom_headers or {}

    async def get_data(self) -> bytes:
        http_response = await self._send_spooling_request(self.uri)
        if not http_response.ok:
            await self._request.raise_response_error(http_response)
        return await http_response.read()

    @property
    def uri(self) -> str:
        return self._segment["uri"]

    @property
    def ack_uri(self) -> str:
        return self._segment["ackUri"]

    @property
    def headers(self) -> dict[str, list[str]]:
        return self._segment.get("headers", {})

    async def acknowledge(self) -> None:
        try:
            http_response = await self._send_spooling_request(self.ack_uri, timeout=2)
            try:
                if not http_response.ok:
                    await self._request.raise_response_error(http_response)
            finally:
                http_response.release()
        except Exception as e:
            logger.error("Failed to acknowledge spooled segment %s: %s", self, e)

    def _is_coordinator(self, uri: str) -> bool:
        return self._coordinator_host is not None and urllib.parse.urlsplit(uri).hostname == self._coordinator_host

    async def _send_spooling_request(self, uri: str, timeout: Optional[float] = None) -> aiohttp.ClientResponse:
        if timeout is None:
            timeout = self._request.segment_client_timeout
        elif not isinstance(timeout, aiohttp.ClientTimeout):
            timeout = aiohttp.ClientTimeout(total=timeout)
        segment_headers: CIMultiDict[str] = CIMultiDict()
        for key, values in self.headers.items():
            for value in values:
                segment_headers.add(key, value)
        # Like the Java client: the segment is already compressed (or incompressible Arrow/zstd data)
        segment_headers.setdefault("Accept-Encoding", "identity")

        # Redirects (COORDINATOR_STORAGE_REDIRECT retrieval mode) are followed manually: the custom headers of the
        # connection are only sent to the coordinator, and nothing else than the segment headers reaches the storage.
        for _ in range(_MAX_SPOOLING_REDIRECTS):
            headers = CIMultiDict(segment_headers)
            if self._is_coordinator(uri):
                # the per-segment headers returned by the coordinator take precedence
                for key, value in self._custom_headers.items():
                    if key not in headers:
                        headers[key] = value
            http_response = await self._request._get(uri, headers=headers, allow_redirects=False, timeout=timeout)
            location = http_response.headers.get("Location")
            if http_response.status not in _REDIRECT_STATUSES or location is None:
                return http_response
            http_response.release()
            uri = str(URL(uri).join(URL(location)))
        raise exceptions.HttpError(f"too many redirects while fetching spooled segment {self}")

    def __repr__(self):
        # the query string of a pre-signed URI is a credential, do not log it
        return f"SpooledSegment(uri={self.uri.split('?', 1)[0]}, metadata={self.metadata})"


class DecodableSegment:
    """
    Represents a collection of spooled segments of data, with an encoding format.

    Attributes:
        encoding (str): The encoding format of the spooled data.
        metadata (_SegmentMetadataTO): Metadata for all segments in the query
        segment (Segment): The spooled segment data
    """

    def __init__(self, encoding: str, metadata: _SegmentMetadataTO, segment: Segment) -> None:
        self._encoding = encoding
        self._metadata = metadata
        self._segment = segment

    @property
    def encoding(self):
        return self._encoding

    @property
    def segment(self):
        return self._segment

    @property
    def metadata(self):
        return self._metadata

    def __repr__(self):
        return f"DecodableSegment(encoding={self._encoding}, metadata={self._metadata}, segment={self._segment})"


class SegmentIterator:
    """Async iterator on the rows of spooled segments, downloaded and decoded lazily.

    Up to ``prefetch`` segments (including the one being served) are downloaded concurrently, in order, while the
    rows are consumed. Decompression, JSON decoding and row mapping run in a worker thread so that they do not block
    the event loop.

    A segment is acknowledged only after its rows were decoded successfully. If loading a segment fails (e.g. the
    download failed), the error is raised and the next call retries the same segment instead of skipping it.
    """

    def __init__(
        self,
        segments: Union[DecodableSegment, list[DecodableSegment]],
        mapper: RowMapper,
        *,
        prefetch: Optional[int] = None,
        query: Optional[TrinoQuery] = None,
    ) -> None:
        """
        :param query: the query of the segments, heartbeats are sent to keep it alive while waiting for a segment
        """
        self._query = query
        self._segments = list(segments) if isinstance(segments, list) else [segments]
        self._mapper = mapper
        self._prefetch = max(1, prefetch if prefetch is not None else constants.DEFAULT_SEGMENT_PREFETCH)
        self._decoder: Optional[QueryDataDecoder] = None
        self._rows: Iterator[list[Any]] = iter([])
        self._finished = False
        self._next_index = 0
        # [segment, task loading it] in segment order; the task is None when loading the segment failed
        self._pending: deque[list[Any]] = deque()

    def __aiter__(self) -> AsyncIterator[list[Any]]:
        return self

    async def __anext__(self) -> list[Any]:
        # If rows are exhausted, fetch the next segment
        while True:
            try:
                return next(self._rows)
            except StopIteration:
                if self._finished:
                    raise StopAsyncIteration from None
                await self._load_next_segment()

    async def _load_next_segment(self) -> None:
        self._schedule()
        if not self._pending:
            self._finished = True
            return
        entry = self._pending[0]
        if entry[1] is None:
            entry[1] = self._start(entry[0])
        try:
            # shield: if the caller is cancelled, the download continues and is awaited by the next call
            if self._query is not None and not entry[1].done():
                async with self._query.keep_alive():
                    rows = await asyncio.shield(entry[1])
            else:
                rows = await asyncio.shield(entry[1])
        except asyncio.CancelledError:
            if entry[1].cancelled():
                entry[1] = None
            raise
        except BaseException:
            entry[1] = None  # retry this segment on the next call
            raise
        self._pending.popleft()
        self._rows = iter(rows)
        self._schedule()

    def _schedule(self) -> None:
        while len(self._pending) < self._prefetch and self._next_index < len(self._segments):
            segment = self._segments[self._next_index]
            self._next_index += 1
            self._pending.append([segment, self._start(segment)])

    def _start(self, segment: DecodableSegment) -> asyncio.Task:
        if self._decoder is None:
            self._decoder = CompressedQueryDataDecoderFactory(self._mapper).create(segment.encoding)
        task = asyncio.create_task(_load_segment(segment, self._decoder.decode, in_thread=True))
        task.add_done_callback(_retrieve_exception)
        return task

    async def aclose(self) -> None:
        """Cancel the downloads in progress."""
        self._finished = True
        self._rows = iter([])
        tasks = [task for _, task in self._pending if task is not None]
        self._pending.clear()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    def __del__(self) -> None:
        for _, task in getattr(self, "_pending", ()):
            if task is not None and not task.done():
                try:
                    task.cancel()
                except RuntimeError:  # event loop already closed
                    pass


def _retrieve_exception(task: asyncio.Task) -> None:
    """Mark the exception of a background task as retrieved, it is re-raised to whoever awaits the task."""
    if not task.cancelled():
        task.exception()


async def _load_segment(
    segment: DecodableSegment,
    decode: Callable[[bytes, _SegmentMetadataTO], Any],
    in_thread: bool = False,
) -> Any:
    """Download, decode (in a worker thread if ``in_thread``) and acknowledge a segment.

    Errors are raised as DB-API errors.
    """
    spooled = segment.segment
    try:
        data = await spooled.get_data()
    except (aiohttp.ClientError, asyncio.TimeoutError, exceptions.HttpError) as e:
        raise exceptions.TrinoSegmentError(f"Failed to download {spooled}: {_describe_error(e)}") from e
    try:
        if in_thread:
            decoded = await asyncio.to_thread(decode, data, spooled.metadata)
        else:
            decoded = decode(data, spooled.metadata)
    except exceptions.Error:
        raise
    except Exception as e:
        raise exceptions.TrinoSegmentError(f"Failed to decode {spooled} ({segment.encoding}): {_describe_error(e)}") from e
    _check_rows_count(spooled, decoded)
    if isinstance(spooled, SpooledSegment):
        await spooled.acknowledge()
    return decoded


def _describe_error(error: BaseException) -> str:
    return f"{type(error).__name__}: {error}" if str(error) else type(error).__name__


def _check_rows_count(segment: Segment, decoded: Any) -> None:
    expected = segment.metadata.get("rowsCount") if segment.metadata else None
    if expected is None:
        return
    actual = decoded.num_rows if hasattr(decoded, "num_rows") else len(decoded)
    if actual != int(expected):
        raise exceptions.TrinoSegmentError(f"{segment} should contain {int(expected)} rows but {actual} rows were decoded")


class SegmentDecoder:
    def __init__(self, decoder: QueryDataDecoder):
        self._decoder = decoder

    async def decode(self, segment: Segment) -> list[list[Any]]:
        if isinstance(segment, InlineSegment):
            inline_segment = cast(InlineSegment, segment)
            return self._decoder.decode(await inline_segment.get_data(), inline_segment.metadata)
        elif isinstance(segment, SpooledSegment):
            spooled_data = cast(SpooledSegment, segment)
            return self._decoder.decode(await spooled_data.get_data(), spooled_data.metadata)
        else:
            raise ValueError(f"Unsupported segment type: {type(segment)}")


class CompressedQueryDataDecoderFactory:
    def __init__(self, mapper: RowMapper) -> None:
        self._mapper = mapper

    def create(self, encoding: str) -> QueryDataDecoder:
        if encoding == "json+zstd":
            if "zstd" in CODECS_UNAVAILABLE:
                raise ValueError(
                    f"zstd is not installed so json+zstd encoding is not supported: {CODECS_UNAVAILABLE['zstd']}"
                )
            return ZStdQueryDataDecoder(JsonQueryDataDecoder(self._mapper))
        elif encoding == "json+lz4":
            if "lz4" in CODECS_UNAVAILABLE:
                raise ValueError(
                    f"lz4 is not installed so json+lz4 encoding is not supported: {CODECS_UNAVAILABLE['lz4']}"
                )
            return Lz4QueryDataDecoder(JsonQueryDataDecoder(self._mapper))
        elif encoding == "json":
            return JsonQueryDataDecoder(self._mapper)
        elif encoding in ("arrow", "arrow+zstd"):
            # zstd buffer compression is part of the Arrow IPC format, handled by pyarrow
            return ArrowQueryDataDecoder(self._mapper)
        else:
            raise ValueError(f"Unsupported encoding: {encoding}")


class QueryDataDecoder(abc.ABC):
    @abstractmethod
    def decode(self, data: bytes, metadata: _SegmentMetadataTO) -> list[list[Any]]:
        pass


class JsonQueryDataDecoder(QueryDataDecoder):
    def __init__(self, mapper: RowMapper) -> None:
        self._mapper = mapper

    def decode(self, data: bytes, metadata: dict[str, Any]) -> list[list[Any]]:
        return self._mapper.map(_json_loads(data))


class ArrowQueryDataDecoder(QueryDataDecoder):
    """Decodes Arrow segments (``arrow``, ``arrow+zstd`` encodings) to rows of the same Python values as the JSON
    encodings (see :func:`aiotrino.arrow.table_to_rows`). Requires pyarrow."""

    def __init__(self, mapper: RowMapper) -> None:
        type_signatures = getattr(mapper, "type_signatures", None)
        if type_signatures is None:
            raise exceptions.NotSupportedError(
                "Arrow encodings cannot be decoded to rows with legacy_primitive_types=True, use a JSON encoding"
            )
        import aiotrino.arrow

        aiotrino.arrow.require_pyarrow()
        self._type_signatures = type_signatures

    def decode(self, data: bytes, metadata: _SegmentMetadataTO) -> list[list[Any]]:
        import pyarrow as pa

        import aiotrino.arrow

        schema, batches = aiotrino.arrow.read_ipc_stream(data)
        return aiotrino.arrow.table_to_rows(pa.Table.from_batches(batches, schema=schema), self._type_signatures)


class CompressedQueryDataDecoder(QueryDataDecoder):
    def __init__(self, delegate: QueryDataDecoder) -> None:
        self._delegate = delegate

    @abstractmethod
    def decompress(self, data: bytes, metadata: _SegmentMetadataTO) -> bytes:
        pass

    def decode(self, data: bytes, metadata: _SegmentMetadataTO) -> list[list[Any]]:
        if "uncompressedSize" not in metadata:
            # Data not compressed - below threshold
            return self._delegate.decode(data, metadata)

        # Data is compressed
        expected_compressed_size = metadata["segmentSize"]
        if not len(data) == expected_compressed_size:
            raise RuntimeError(f"Expected to read {expected_compressed_size} bytes but got {len(data)}")
        decompressed_data = self.decompress(data, metadata)
        expected_uncompressed_size = metadata["uncompressedSize"]
        if not len(decompressed_data) == expected_uncompressed_size:
            raise RuntimeError(
                "Decompressed size does not match expected segment size, "
                f"expected {expected_uncompressed_size}, got {len(decompressed_data)}"
            )
        return self._delegate.decode(decompressed_data, metadata)


class ZStdQueryDataDecoder(CompressedQueryDataDecoder):
    def decompress(self, data: bytes, metadata: _SegmentMetadataTO) -> bytes:
        # ZstdDecompressor instances are not thread-safe and segments are decoded in worker threads: use one per call
        return zstandard.ZstdDecompressor().decompress(data, max_output_size=int(metadata["uncompressedSize"]))


class Lz4QueryDataDecoder(CompressedQueryDataDecoder):
    def decompress(self, data: bytes, metadata: _SegmentMetadataTO) -> bytes:
        expected_uncompressed_size = metadata["uncompressedSize"]
        decoded_bytes = lz4.block.decompress(data, uncompressed_size=int(expected_uncompressed_size))
        return decoded_bytes
