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
"""``fixtures/types_arrow.arrow``: an Arrow segment written by Java Arrow 19 with the schema of the server's
TrinoToArrowTypeConverter (before INTERVAL YEAR TO MONTH switched from ``month_interval`` to ``month_day_nano_interval``).
It contains two rows: the values of ``FIXTURE_CASES`` (as sent by the JSON encodings), then only nulls."""
import os

from aiotrino.mapper import RowMapperFactory


def read_fixture() -> bytes:
    with open(os.path.join(os.path.dirname(__file__), "fixtures", "types_arrow.arrow"), "rb") as f:
        return f.read()


def sig(raw_type, *arguments):
    return {"rawType": raw_type, "arguments": list(arguments)}


def long_arg(value):
    return {"kind": "LONG", "value": value}


def type_arg(type_signature):
    return {"kind": "TYPE", "value": type_signature}


def named(name, type_signature):
    value = {"typeSignature": type_signature}
    if name is not None:
        value["fieldName"] = {"name": name}
    return {"kind": "NAMED_TYPE", "value": value}


VARCHAR = sig("varchar", long_arg(2147483647))

# (column name, Trino type, type signature, value of the first row with the JSON encodings)
FIXTURE_CASES = [
    ("c_boolean", "boolean", sig("boolean"), True),
    ("c_tinyint", "tinyint", sig("tinyint"), -5),
    ("c_smallint", "smallint", sig("smallint"), 300),
    ("c_integer", "integer", sig("integer"), 70000),
    ("c_bigint", "bigint", sig("bigint"), 1 << 40),
    ("c_real", "real", sig("real"), 1.5),
    ("c_double", "double", sig("double"), 2.25),
    ("c_varchar", "varchar", VARCHAR, "héllo"),
    ("c_char5", "char(5)", sig("char", long_arg(5)), "ab   "),
    ("c_varbinary", "varbinary", sig("varbinary"), "AAEC/w=="),
    ("c_json", "json", sig("json"), '{"a":[1,2]}'),
    ("c_date", "date", sig("date"), "2024-01-01"),
    ("c_time0", "time(0)", sig("time", long_arg(0)), "01:02:03"),
    ("c_time3", "time(3)", sig("time", long_arg(3)), "01:02:03.456"),
    ("c_time6", "time(6)", sig("time", long_arg(6)), "01:02:03.456789"),
    ("c_time9", "time(9)", sig("time", long_arg(9)), "01:02:03.456789123"),
    ("c_timetz3", "time(3) with time zone", sig("time with time zone", long_arg(3)), "10:00:00.123+05:30"),
    ("c_ts0", "timestamp(0)", sig("timestamp", long_arg(0)), "2024-01-02 03:04:05"),
    ("c_ts3", "timestamp(3)", sig("timestamp", long_arg(3)), "2024-01-02 03:04:05.123"),
    ("c_ts6", "timestamp(6)", sig("timestamp", long_arg(6)), "2024-01-02 03:04:05.123456"),
    ("c_ts9", "timestamp(9)", sig("timestamp", long_arg(9)), "2024-01-02 03:04:05.123456789"),
    ("c_tstz3", "timestamp(3) with time zone", sig("timestamp with time zone", long_arg(3)),
     "2024-01-02 04:04:05.123 Europe/Paris"),
    ("c_tstz9", "timestamp(9) with time zone", sig("timestamp with time zone", long_arg(9)),
     "2024-01-02 04:04:05.123456789 Europe/Paris"),
    ("c_decimal_10_2", "decimal(10,2)", sig("decimal", long_arg(10), long_arg(2)), "12345.67"),
    ("c_decimal_38_10", "decimal(38,10)", sig("decimal", long_arg(38), long_arg(10)), "1234567890123456789012345678.0123456789"),
    ("c_uuid", "uuid", sig("uuid"), "12151fd2-7586-11e9-8f9e-2a86e4085a59"),
    # microsecond values, as sent with the PARAMETRIC_INTERVAL capability (the legacy format has milliseconds)
    ("c_interval_ds", "interval day to second", sig("interval day to second"), "1 02:03:04.123456"),
    ("c_interval_ds_neg", "interval day to second", sig("interval day to second"), "-0 00:00:01.500500"),
    ("c_interval_ym", "interval year to month", sig("interval year to month"), "1-2"),
    ("c_array", "array(bigint)", sig("array", type_arg(sig("bigint"))), [1, None, 3]),
    ("c_map", "map(varchar, bigint)", sig("map", type_arg(VARCHAR), type_arg(sig("bigint"))), {"a": 1, "b": 2}),
    ("c_row", "row(x bigint, y varchar)", sig("row", named("x", sig("bigint")), named("y", VARCHAR)), [1, "z"]),
    ("c_row_anon", "row(bigint, bigint)", sig("row", named(None, sig("bigint")), named(None, sig("bigint"))), [7, 8]),
    ("c_unknown", "unknown", sig("unknown"), None),
    ("c_hll", "HyperLogLog", sig("HyperLogLog"), "AgwA"),
]

FIXTURE_COLUMNS = [{"name": name, "type": type_name, "typeSignature": type_signature}
                   for name, type_name, type_signature, _ in FIXTURE_CASES]
FIXTURE_JSON_ROW = [value for *_, value in FIXTURE_CASES]
# columns with nanosecond values, which Python's datetime/time cannot represent
NANOSECOND_COLUMNS = {"c_time9", "c_ts9", "c_tstz9"}


def fixture_rows():
    """The two rows of the fixture, mapped to Python values like the rows of the JSON encodings."""
    mapper = RowMapperFactory().create(columns=FIXTURE_COLUMNS, legacy_primitive_types=False)
    return mapper.map([FIXTURE_JSON_ROW, [None] * len(FIXTURE_JSON_ROW)])
