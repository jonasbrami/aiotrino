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
"""Tests of aiotrino.arrow: Trino to Arrow type mapping and conversions."""
import uuid
from datetime import datetime, time

import pytest

from aiotrino.mapper import RowMapperFactory
from tests.unit.arrow_fixtures import (
    FIXTURE_COLUMNS,
    NANOSECOND_COLUMNS,
    fixture_rows,
    long_arg,
    named,
    read_fixture,
    sig,
    type_arg,
)

pa = pytest.importorskip("pyarrow")

from aiotrino.arrow import (  # noqa: E402
    TRINO_TYPE_METADATA_KEY,
    read_ipc_stream,
    rows_to_record_batch,
    table_to_rows,
    to_arrow_schema,
    to_arrow_type,
)


def test_client_conversions_match_the_server_segment():
    """The client maps the Trino types like the server, and converts the rows of the JSON encodings to the same Arrow
    data as the server's Arrow encoding."""
    schema, batches = read_ipc_stream(read_fixture())
    server = pa.Table.from_batches(batches, schema=schema)
    # INTERVAL YEAR TO MONTH, sent as month_interval by older servers, is read as month_day_nano_interval
    assert server.column("c_interval_ym").to_pylist() == [pa.MonthDayNano([14, 0, 0]), None]

    client_schema = to_arrow_schema(FIXTURE_COLUMNS)
    for client_field, server_field in zip(client_schema, schema, strict=True):
        assert client_field.equals(server_field), client_field.name
    assert client_schema.field("c_char5").metadata == {TRINO_TYPE_METADATA_KEY: b"char(5)"}

    client = rows_to_record_batch(fixture_rows(), FIXTURE_COLUMNS)
    for name in server.column_names:
        server_column, client_column = server.column(name).combine_chunks(), client.column(name)
        if name in NANOSECOND_COLUMNS:
            # Python's temporal types have microsecond precision: the JSON path rounds the nanoseconds
            server_nanos, client_nanos = server_column.cast(pa.int64()), client_column.cast(pa.int64())
            assert abs(client_nanos[0].as_py() - server_nanos[0].as_py()) <= 500, name
            assert not client_nanos[1].is_valid
        else:
            assert client_column.equals(server_column), name


@pytest.mark.parametrize(
    "type_signature, expected",
    [
        (sig("time"), pa.time32("ms")),
        (sig("qdigest", type_arg(sig("bigint"))), pa.binary()),
        # like the server, the fields of a row used as map key are not nullable
        (
            sig("map", type_arg(sig("row", named("k", sig("bigint")))), type_arg(sig("bigint"))),
            pa.map_(pa.struct([pa.field("k", pa.int64(), nullable=False)]), pa.int64()),
        ),
    ],
)
def test_to_arrow_type(type_signature, expected):
    assert to_arrow_type(type_signature) == expected


def test_rows_to_arrow_values():
    columns = [
        # a type without Arrow equivalent: its representation with the JSON encodings
        {"name": "ip", "type": "ipaddress", "typeSignature": sig("ipaddress")},
        # extension types (uuid) in nested types
        {"name": "uuids", "type": "array(uuid)", "typeSignature": sig("array", type_arg(sig("uuid")))},
    ]
    rows = RowMapperFactory().create(columns=columns, legacy_primitive_types=False).map(
        [["10.0.0.1", ["12151fd2-7586-11e9-8f9e-2a86e4085a59", None]]]
    )
    assert pa.Table.from_batches([rows_to_record_batch(rows, columns)]).to_pylist() == [
        {"ip": "10.0.0.1", "uuids": [uuid.UUID("12151fd2-7586-11e9-8f9e-2a86e4085a59"), None]},
    ]


def test_table_to_rows_nested_values():
    columns = [
        {"name": "c", "type": "array(map(varchar, row(t time(9), r real)))",
         "typeSignature": sig("array", type_arg(sig("map", type_arg(sig("varchar")), type_arg(
             sig("row", named("t", sig("time", long_arg(9))), named(None, sig("real")))))))},
        {"name": "ts", "type": "timestamp(9)", "typeSignature": sig("timestamp", long_arg(9))},
    ]
    schema = to_arrow_schema(columns)
    table = pa.table({
        "c": pa.array([
            [[("a", {"t": 1_000_000_500, "field1": 1.1}), ("b", None)], None],
            None,
            [[], [("c", {"t": None, "field1": None})]],
        ], schema.field("c").type),
        "ts": pa.array([1_500, None, -1_500], pa.timestamp("ns")),
    }, schema=schema)
    type_signatures = [column["typeSignature"] for column in columns]
    rows = table_to_rows(table, type_signatures)
    assert rows[0][0] == [{"a": (time(0, 0, 1, 0), 1.1), "b": None}, None]
    assert rows[0][0][0]["a"].t == time(0, 0, 1)
    assert rows[1] == [None, None]
    assert rows[2][0] == [{}, {"c": (None, None)}]
    # nanoseconds are rounded half to even
    assert rows[0][1] == datetime(1970, 1, 1, 0, 0, 0, 2)
    assert rows[2][1] == datetime(1969, 12, 31, 23, 59, 59, 999998)
    # sliced tables
    assert table_to_rows(table.slice(2), type_signatures) == rows[2:]


def test_rows_to_arrow_with_duplicate_row_field_names():
    columns = [
        {"name": "r", "type": "row(field1 integer, integer)",
         "typeSignature": sig("row", named("field1", sig("integer")), named(None, sig("integer")))},
        {"name": "d", "type": "row(a integer, a integer)",
         "typeSignature": sig("row", named("a", sig("integer")), named("a", sig("integer")))},
    ]
    mapper = RowMapperFactory().create(columns=columns, legacy_primitive_types=False)
    batch = rows_to_record_batch(mapper.map([[[1, 2], [3, 4]], [None, None]]), columns)
    assert batch.schema.field("r").type == pa.struct([("field1", pa.int32()), ("field1", pa.int32())])
    assert [child.to_pylist() for child in batch.column(0).flatten()] == [[1, None], [2, None]]
    assert [child.to_pylist() for child in batch.column(1).flatten()] == [[3, None], [4, None]]
