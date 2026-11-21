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
"""Apache Arrow support (requires ``pyarrow``).

- :func:`to_arrow_schema` maps the columns of a Trino result to an Arrow schema. It mirrors the server-side
  ``TrinoToArrowTypeConverter`` used by the ``arrow`` and ``arrow+zstd`` spooling encodings (list children are named
  ``element``, map entries ``entries`` with ``key`` and ``value`` children, anonymous row fields ``field0``, ...), so
  that the schema of an empty result, or of a result received with a JSON encoding, is equal to the schema of a
  result received as Arrow.
- :func:`rows_to_record_batch` converts rows mapped by :class:`aiotrino.mapper.RowMapper` (JSON encodings, direct
  protocol) to Arrow.
- :func:`read_ipc_stream` decodes an Arrow IPC stream (a segment of the Arrow encodings).
"""
from __future__ import annotations

import base64
import uuid
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable, Optional
from zoneinfo import ZoneInfo

from dateutil.relativedelta import relativedelta

from aiotrino.types import NamedRowTuple

try:
    import pyarrow as pa
    import pyarrow.ipc as ipc
except ImportError:  # pragma: no cover - exercised without pyarrow
    pa = None
    ipc = None

__all__ = [
    "TRINO_TYPE_METADATA_KEY",
    "require_pyarrow",
    "to_arrow_type",
    "to_arrow_field",
    "to_arrow_schema",
    "rows_to_record_batch",
    "read_ipc_stream",
    "table_to_rows",
    "table_to_pandas",
    "table_to_polars",
]

# Metadata key of the fields of the schemas built by aiotrino, holding the Trino type of the column
TRINO_TYPE_METADATA_KEY = b"trino.type"

# Trino types without Arrow equivalent, received as base64 strings with the JSON encodings
_BINARY_FALLBACK_TYPES = {"hyperloglog", "p4hyperloglog", "qdigest", "tdigest", "setdigest", "khyperloglog"}


def require_pyarrow():
    """Return the ``pyarrow`` module, raise an ImportError explaining how to install it if it is missing."""
    if pa is None:
        raise ImportError(
            "PyArrow is required for Arrow functionality but is not installed. "
            "Install it with: pip install 'pyarrow>=18' (or the aiotrino[arrow] extra)"
        )
    return pa


# Trino type -> Arrow type -----------------------------------------------------------------------------------------


def _arguments(type_signature: dict[str, Any]) -> list[dict[str, Any]]:
    return type_signature.get("arguments") or []


def _long_argument(type_signature: dict[str, Any], index: int, default: int) -> int:
    arguments = _arguments(type_signature)
    if len(arguments) <= index:
        return default
    return int(arguments[index]["value"])


def _type_argument(type_signature: dict[str, Any], index: int) -> dict[str, Any]:
    return _arguments(type_signature)[index]["value"]


def _row_fields(type_signature: dict[str, Any]) -> list[tuple[Optional[str], dict[str, Any]]]:
    fields = []
    for argument in _arguments(type_signature):
        value = argument["value"]
        name = value["fieldName"]["name"] if value.get("fieldName") else None
        fields.append((name, value["typeSignature"]))
    return fields


def _time_unit(precision: int) -> str:
    # Trino precisions 0, 3, 6, 9, 12 map to s, ms, us, ns, ns (picoseconds are truncated to nanoseconds)
    if precision == 0:
        return "s"
    if precision <= 3:
        return "ms"
    if precision <= 6:
        return "us"
    return "ns"


def _uuid_type():
    return pa.uuid() if hasattr(pa, "uuid") else pa.binary(16)


def _primitive_arrow_type(type_signature: dict[str, Any]):
    raw_type = type_signature["rawType"].lower()
    simple = {
        "boolean": pa.bool_(),
        "tinyint": pa.int8(),
        "smallint": pa.int16(),
        "integer": pa.int32(),
        "bigint": pa.int64(),
        "real": pa.float32(),
        "double": pa.float64(),
        "varchar": pa.string(),
        "char": pa.string(),
        "varbinary": pa.binary(),
        "json": pa.string(),
        "date": pa.date32(),
        "uuid": _uuid_type(),
        # Day-time intervals are encoded as microseconds
        "interval day to second": pa.duration("us"),
        # Year-month intervals are encoded as a number of months (days and nanoseconds are 0)
        "interval year to month": pa.month_day_nano_interval(),
        "unknown": pa.null(),
    }
    if raw_type in simple:
        return simple[raw_type]
    if raw_type in ("time", "time with time zone"):
        # TIME WITH TIME ZONE values are normalized to UTC, the offset is lost
        unit = _time_unit(_long_argument(type_signature, 0, 3))
        return pa.time32(unit) if unit in ("s", "ms") else pa.time64(unit)
    if raw_type == "timestamp":
        return pa.timestamp(_time_unit(_long_argument(type_signature, 0, 3)))
    if raw_type == "timestamp with time zone":
        # the instant is kept, the time zone of the values is lost
        return pa.timestamp(_time_unit(_long_argument(type_signature, 0, 3)), tz="UTC")
    if raw_type == "decimal":
        return pa.decimal128(_long_argument(type_signature, 0, 38), _long_argument(type_signature, 1, 0))
    # Types the server cannot encode as Arrow (a query returning them fails with the Arrow encodings): use the
    # representation of the JSON encodings.
    if raw_type in _BINARY_FALLBACK_TYPES:
        return pa.binary()
    return pa.string()


def to_arrow_field(name: str, type_signature: dict[str, Any], nullable: bool = True):
    """Arrow field of a Trino type signature (``column["typeSignature"]``), like the server's converter."""
    require_pyarrow()
    raw_type = type_signature["rawType"].lower()
    if raw_type == "array":
        element = to_arrow_field("element", _type_argument(type_signature, 0), True)
        return pa.field(name, pa.list_(element), nullable=True)
    if raw_type == "map":
        # Arrow requires a non-nullable key field (the server's converter makes array and row fields always nullable)
        key = to_arrow_field("key", _type_argument(type_signature, 0), False).with_nullable(False)
        value = to_arrow_field("value", _type_argument(type_signature, 1), nullable)
        return pa.field(name, pa.map_(key, value), nullable=True)
    if raw_type == "row":
        children = [
            to_arrow_field(field_name if field_name is not None else f"field{index}", field_signature, nullable)
            for index, (field_name, field_signature) in enumerate(_row_fields(type_signature))
        ]
        return pa.field(name, pa.struct(children), nullable=True)
    return pa.field(name, _primitive_arrow_type(type_signature), nullable=nullable)


def to_arrow_type(type_signature: dict[str, Any]):
    """Arrow type of a Trino type signature (``column["typeSignature"]``)."""
    return to_arrow_field("", type_signature).type


def to_arrow_schema(columns: list[dict[str, Any]], trino_metadata: bool = True):
    """Arrow schema of the columns of a Trino result (``cursor._query.columns``).

    :param trino_metadata: add the Trino type of each column to the field metadata (key ``trino.type``)
    """
    require_pyarrow()
    fields = []
    for column in columns:
        field = to_arrow_field(column["name"], column["typeSignature"])
        if trino_metadata and column.get("type"):
            field = field.with_metadata({TRINO_TYPE_METADATA_KEY: str(column["type"]).encode()})
        fields.append(field)
    return pa.schema(fields)


# rows (Python values of the JSON path) -> Arrow ---------------------------------------------------------------------


_EPOCH_DATE = date(1970, 1, 1)


def _value_converter(arrow_type, type_signature: dict[str, Any]) -> Optional[Callable[[Any], Any]]:
    """Return a function converting a value of the JSON path (aiotrino.mapper) to a value accepted by pyarrow for
    ``arrow_type``, or None when the value can be used as is."""
    raw_type = type_signature["rawType"].lower()

    if pa.types.is_list(arrow_type):
        element = _value_converter(arrow_type.value_type, _type_argument(type_signature, 0))
        if element is None:
            return None
        return lambda value: None if value is None else [element(item) for item in value]

    if pa.types.is_map(arrow_type):
        key = _value_converter(arrow_type.key_type, _type_argument(type_signature, 0)) or _identity
        item = _value_converter(arrow_type.item_type, _type_argument(type_signature, 1)) or _identity
        return lambda value: None if value is None else [(key(k), item(v)) for k, v in value.items()]

    if pa.types.is_struct(arrow_type):
        names = [arrow_type.field(i).name for i in range(arrow_type.num_fields)]
        converters = [
            _value_converter(arrow_type.field(i).type, field_signature) or _identity
            for i, (_, field_signature) in enumerate(_row_fields(type_signature))
        ]

        def convert_row(value):
            if value is None:
                return None
            # rows are NamedRowTuple instances, field names may be missing or duplicated: use positions
            return {name: converter(item) for name, converter, item in zip(names, converters, value, strict=True)}

        return convert_row

    if isinstance(arrow_type, pa.BaseExtensionType) or pa.types.is_fixed_size_binary(arrow_type):  # uuid
        return lambda value: None if value is None else (value if isinstance(value, bytes) else uuid.UUID(str(value)).bytes)

    if pa.types.is_interval(arrow_type):  # interval year to month
        def convert_interval(value):
            if value is None:
                return None
            if isinstance(value, int):
                return (value, 0, 0)
            return (value.years * 12 + value.months, 0, 0)  # dateutil.relativedelta

        return convert_interval

    if pa.types.is_time(arrow_type) and raw_type == "time with time zone":
        # like the server: the time of day normalized to UTC, without offset
        def convert_time_with_time_zone(value):
            if value is None or value.tzinfo is None:
                return value
            offset = value.utcoffset() or timedelta(0)
            utc = datetime.combine(_EPOCH_DATE, value.replace(tzinfo=None)) - offset
            return utc.time()

        return convert_time_with_time_zone

    if pa.types.is_timestamp(arrow_type) and arrow_type.tz is not None:
        return lambda value: value if value is None or value.tzinfo is not None else value.replace(tzinfo=timezone.utc)

    if pa.types.is_binary(arrow_type) and raw_type != "varbinary":
        # HyperLogLog & co. are base64 strings with the JSON encodings
        return lambda value: base64.b64decode(value) if isinstance(value, str) else value

    if pa.types.is_string(arrow_type) and raw_type not in ("varchar", "char", "json"):
        # types without Arrow equivalent (ipaddress, geometry, ...): their JSON representation
        return lambda value: value if value is None or isinstance(value, str) else str(value)

    if pa.types.is_null(arrow_type):
        return lambda value: None

    return None


def _identity(value: Any) -> Any:
    return value


def _storage_type(arrow_type):
    """``arrow_type`` with extension types (uuid) replaced by their storage type: pyarrow cannot build nested arrays
    of extension types from Python values."""
    if isinstance(arrow_type, pa.BaseExtensionType):
        return arrow_type.storage_type
    if pa.types.is_list(arrow_type):
        field = arrow_type.value_field
        return pa.list_(field.with_type(_storage_type(field.type)))
    if pa.types.is_map(arrow_type):
        key, item = arrow_type.key_field, arrow_type.item_field
        return pa.map_(key.with_type(_storage_type(key.type)), item.with_type(_storage_type(item.type)))
    if pa.types.is_struct(arrow_type):
        return pa.struct([field.with_type(_storage_type(field.type)) for field in arrow_type])
    return arrow_type


def _to_array(values: list[Any], arrow_type):
    storage_type = _storage_type(arrow_type)
    array = pa.array(values, type=storage_type)
    if storage_type != arrow_type:
        # zero-copy: the extension types have the same layout as their storage type
        array = array.view(arrow_type)
    return array


def rows_to_record_batch(rows: list[list[Any]], columns: list[dict[str, Any]], schema=None):
    """Convert rows mapped to Python values by :class:`aiotrino.mapper.RowMapper` (JSON encodings, direct protocol)
    to an Arrow record batch of ``schema`` (by default :func:`to_arrow_schema` of ``columns``)."""
    require_pyarrow()
    if schema is None:
        schema = to_arrow_schema(columns)
    arrays = []
    for index, (field, column) in enumerate(zip(schema, columns, strict=True)):
        converter = _value_converter(field.type, column["typeSignature"])
        if converter is None:
            values = [row[index] for row in rows]
        else:
            values = [converter(row[index]) for row in rows]
        arrays.append(_to_array(values, field.type))
    return pa.RecordBatch.from_arrays(arrays, schema=schema)


# Arrow IPC ----------------------------------------------------------------------------------------------------------


def read_ipc_stream(data: bytes):
    """Decode an Arrow IPC stream (a segment of the ``arrow``/``arrow+zstd`` encodings).

    Returns the schema and the record batches. ``month_interval`` columns (INTERVAL YEAR TO MONTH as sent by servers
    before they switched to ``month_day_nano_interval``), which pyarrow cannot read, are converted to
    ``month_day_nano_interval``.
    """
    require_pyarrow()
    reader = ipc.open_stream(pa.py_buffer(data))
    batches = list(reader)
    schema = reader.schema
    if _contains_month_interval(schema):
        batches = [_convert_month_intervals(batch) for batch in batches]
        schema = _month_interval_to(schema, pa.month_day_nano_interval())
    return schema, batches


def _is_month_interval(arrow_type) -> bool:
    # no pyarrow constant for this type id in pyarrow < 19, and no Python class for its arrays
    return str(arrow_type) == "month_interval"


def _contains_month_interval(schema) -> bool:
    return any(_type_contains(field.type, _is_month_interval) for field in schema)


def _type_contains(arrow_type, predicate) -> bool:
    if predicate(arrow_type):
        return True
    return any(_type_contains(arrow_type.field(i).type, predicate) for i in range(arrow_type.num_fields))


def _replace_type(arrow_type, predicate, replacement):
    if predicate(arrow_type):
        return replacement
    if pa.types.is_list(arrow_type):
        field = arrow_type.value_field
        return pa.list_(field.with_type(_replace_type(field.type, predicate, replacement)))
    if pa.types.is_map(arrow_type):
        key, item = arrow_type.key_field, arrow_type.item_field
        return pa.map_(
            key.with_type(_replace_type(key.type, predicate, replacement)),
            item.with_type(_replace_type(item.type, predicate, replacement)),
        )
    if pa.types.is_struct(arrow_type):
        return pa.struct([field.with_type(_replace_type(field.type, predicate, replacement)) for field in arrow_type])
    return arrow_type


def _month_interval_to(schema, replacement):
    return pa.schema(
        [field.with_type(_replace_type(field.type, _is_month_interval, replacement)) for field in schema],
        metadata=schema.metadata,
    )


def _convert_month_intervals(batch):
    # pyarrow has no Python class for month_interval arrays, any access to such a column fails: view the batch with
    # int32 (same layout) instead of month_interval, then convert the number of months to month_day_nano_interval.
    int32_schema = _month_interval_to(batch.schema, pa.int32())
    struct = batch.to_struct_array().view(pa.struct(list(int32_schema)))
    columns = [
        _convert_leaves(struct.field(i), field.type, _is_month_interval, _months_to_month_day_nano,
                        pa.month_day_nano_interval())
        for i, field in enumerate(batch.schema)
    ]
    return pa.RecordBatch.from_arrays(columns, schema=_month_interval_to(batch.schema, pa.month_day_nano_interval()))


def _months_to_month_day_nano(array):
    return pa.array([None if months is None else (months, 0, 0) for months in array.to_pylist()],
                    pa.month_day_nano_interval())


def _convert_leaves(array, original_type, predicate, convert, new_leaf_type):
    """Rebuild ``array`` (of ``original_type``, possibly viewed with another type of the same layout) with
    ``convert(leaf_array)`` for every (nested) value whose original type matches ``predicate``."""
    if not _type_contains(original_type, predicate):
        return array
    if predicate(original_type):
        return convert(array)
    mask = array.is_null() if array.null_count else None
    new_type = _replace_type(original_type, predicate, new_leaf_type)
    if pa.types.is_map(original_type):
        keys = _convert_leaves(array.keys, original_type.key_type, predicate, convert, new_leaf_type)
        items = _convert_leaves(array.items, original_type.item_type, predicate, convert, new_leaf_type)
        return pa.MapArray.from_arrays(array.offsets, keys, items, mask=mask, type=new_type)
    if pa.types.is_list(original_type):
        values = _convert_leaves(array.values, original_type.value_type, predicate, convert, new_leaf_type)
        return pa.ListArray.from_arrays(array.offsets, values, mask=mask, type=new_type)
    if pa.types.is_struct(original_type):
        children = [
            _convert_leaves(array.field(i), original_type.field(i).type, predicate, convert, new_leaf_type)
            for i in range(original_type.num_fields)
        ]
        return pa.StructArray.from_arrays(children, fields=list(new_type), mask=mask)
    return array


# pandas / polars ----------------------------------------------------------------------------------------------------


def _is_nanosecond_time(arrow_type) -> bool:
    return pa.types.is_time64(arrow_type) and arrow_type.unit == "ns"


def table_to_pandas(table, *, arrow_dtypes: bool = False, types_mapper=None, **kwargs):
    """``table.to_pandas()``, with ``pandas.ArrowDtype`` for the columns pandas cannot convert without losing data:
    TIME(9)/TIME(12) values with sub-microsecond digits fail with "Value ... has non-zero nanoseconds".

    :param arrow_dtypes: use ``pandas.ArrowDtype`` for every column
    :param types_mapper: passed to ``to_pandas()``, replaces the default mapping
    """
    import pandas as pd

    if types_mapper is None:
        if arrow_dtypes:
            types_mapper = pd.ArrowDtype
        else:
            def types_mapper(arrow_type):
                return pd.ArrowDtype(arrow_type) if _type_contains(arrow_type, _is_nanosecond_time) else None
    return table.to_pandas(types_mapper=types_mapper, **kwargs)


def _is_month_day_nano(arrow_type) -> bool:
    return pa.types.is_interval(arrow_type)


def _month_day_nano_to_months(array):
    return pa.array([None if value is None else value.months for value in array.to_pylist()], pa.int32())


def table_to_polars(table):
    """``polars.from_arrow(table)``. polars cannot import ``month_day_nano_interval`` (INTERVAL YEAR TO MONTH): those
    values are converted to their number of months (Int32)."""
    import polars as pl

    if any(_type_contains(field.type, _is_month_day_nano) for field in table.schema):
        columns = []
        fields = []
        for field, column in zip(table.schema, table.columns, strict=True):
            if _type_contains(field.type, _is_month_day_nano):
                chunks = [_convert_leaves(chunk, field.type, _is_month_day_nano, _month_day_nano_to_months, pa.int32())
                          for chunk in column.chunks]
                new_type = _replace_type(field.type, _is_month_day_nano, pa.int32())
                column = pa.chunked_array(chunks, type=new_type)
                field = field.with_type(new_type)
            columns.append(column)
            fields.append(field)
        table = pa.Table.from_arrays(columns, schema=pa.schema(fields, metadata=table.schema.metadata))
    return pl.from_arrow(table)


# Arrow -> rows (Python values of the JSON path) ---------------------------------------------------------------------


def table_to_rows(table, type_signatures: list[dict[str, Any]]) -> list[list[Any]]:
    """Convert an Arrow table (or record batch) to rows of the same Python values as the JSON encodings give with
    :class:`aiotrino.mapper.RowMapper`: maps as dicts, rows as :class:`aiotrino.types.NamedRowTuple`, nanoseconds
    rounded to microseconds, REAL values as their shortest decimal representation, sketches (HyperLogLog, ...) as
    base64 strings, ...

    Differences with the JSON encodings, due to the Arrow encoding of the server: TIME WITH TIME ZONE values are in
    UTC (the offset is lost) and TIMESTAMP WITH TIME ZONE values are in UTC (the time zone is lost, the instant is
    kept).
    """
    require_pyarrow()
    if table.num_columns != len(type_signatures):
        raise ValueError(f"Expected {len(type_signatures)} columns, got {table.num_columns}")
    columns = []
    for index, type_signature in enumerate(type_signatures):
        column = table.column(index)
        chunks = column.chunks if hasattr(column, "chunks") else [column]
        values: list[Any] = []
        for chunk in chunks:
            values.extend(_array_to_python(chunk, type_signature))
        columns.append(values)
    return [list(row) for row in zip(*columns, strict=True)] if columns else [[] for _ in range(table.num_rows)]


_EPOCH = datetime(1970, 1, 1)
_UTC = ZoneInfo("UTC")


def _round_nanos_to_micros(nanos: int) -> int:
    # half to even, like the JSON path (Decimal.quantize)
    micros, remainder = divmod(nanos, 1000)
    if remainder > 500 or (remainder == 500 and micros % 2 == 1):
        micros += 1
    return micros


def _array_to_python(array, type_signature: dict[str, Any]) -> list[Any]:
    arrow_type = array.type
    raw_type = type_signature["rawType"].lower()

    if pa.types.is_map(arrow_type):
        # keys and items are the whole children, offsets account for slicing (and for null maps backed by entries)
        keys = _array_to_python(array.keys, _type_argument(type_signature, 0))
        items = _array_to_python(array.items, _type_argument(type_signature, 1))
        offsets = array.offsets.to_pylist()
        valid = array.is_valid().to_pylist()
        return [
            dict(zip(keys[offsets[i]:offsets[i + 1]], items[offsets[i]:offsets[i + 1]], strict=True)) if valid[i] else None
            for i in range(len(array))
        ]

    if pa.types.is_list(arrow_type) or pa.types.is_large_list(arrow_type):
        values = _array_to_python(array.flatten(), _type_argument(type_signature, 0))
        result = []
        position = 0
        for length in array.value_lengths().to_pylist():
            if length is None:
                result.append(None)
            else:
                result.append(values[position:position + length])
                position += length
        return result

    if pa.types.is_struct(arrow_type):
        fields = _row_fields(type_signature)
        names = [name for name, _ in fields]
        types = [field_signature["rawType"] for _, field_signature in fields]
        children = [
            _array_to_python(child, field_signature)
            for child, (_, field_signature) in zip(array.flatten(), fields, strict=True)
        ]
        valid = array.is_valid().to_pylist()
        return [
            NamedRowTuple([child[i] for child in children], names, types) if valid[i] else None
            for i in range(len(array))
        ]

    if pa.types.is_timestamp(arrow_type):
        tz = _UTC if arrow_type.tz is not None else None
        if arrow_type.unit != "ns":
            values = array.to_pylist()
            if tz is not None:
                values = [None if value is None else value.astimezone(_UTC) for value in values]
            return values
        epoch = _EPOCH.replace(tzinfo=tz)
        return [
            None if nanos is None else epoch + timedelta(microseconds=_round_nanos_to_micros(nanos))
            for nanos in array.cast(pa.int64()).to_pylist()
        ]

    if pa.types.is_time(arrow_type):
        if arrow_type.unit == "ns":
            values = [
                None if nanos is None else (_EPOCH + timedelta(microseconds=_round_nanos_to_micros(nanos))).time()
                for nanos in array.cast(pa.int64()).to_pylist()
            ]
        else:
            values = array.to_pylist()
        if raw_type == "time with time zone":
            # the server normalizes the values to UTC
            values = [None if value is None else value.replace(tzinfo=timezone.utc) for value in values]
        return values

    if pa.types.is_interval(arrow_type):  # month_day_nano_interval: interval year to month
        return [None if value is None else relativedelta(months=value.months) for value in array.to_pylist()]

    if pa.types.is_float32(arrow_type):
        # the JSON encoding sends the shortest representation of the float32 value
        return [None if value is None else float(value) for value in array.cast(pa.string()).to_pylist()]

    if isinstance(arrow_type, pa.BaseExtensionType) or pa.types.is_fixed_size_binary(arrow_type):  # uuid
        return [
            value if value is None or isinstance(value, uuid.UUID) else uuid.UUID(bytes=bytes(value))
            for value in array.to_pylist()
        ]

    if pa.types.is_binary(arrow_type) and raw_type != "varbinary":
        # HyperLogLog & co. are base64 strings with the JSON encodings
        return [None if value is None else base64.b64encode(value).decode() for value in array.to_pylist()]

    return array.to_pylist()
