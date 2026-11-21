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
"""Tests of the ADBC-style API of the cursors and connections."""
import pickle

import pytest

import aiotrino
from aiotrino import constants, dbapi
from tests.unit.arrow_fixtures import FIXTURE_COLUMNS, read_fixture
from tests.unit.fake_trino import COLUMNS, FakeTrino, Segment, arrow_table, json_rows

pa = pytest.importorskip("pyarrow")

import aiotrino.arrow  # noqa: E402


async def test_description_and_rownumber():
    async with FakeTrino() as trino:
        trino.add_page(columns=COLUMNS, data=json_rows(0, 5))
        async with trino.cursor(encoding=None) as cur:
            assert (cur.description, cur.arrow_schema, cur.rownumber) == (None, None, None)
            with pytest.raises(dbapi.ProgrammingError):
                await cur.fetch_arrow_table()
            await cur.execute("SELECT 1")
            # available without awaiting get_description()
            description = cur.description
            assert description == await cur.get_description()
            assert [(column.name, column.type_code) for column in description] == [("a", "bigint"), ("s", "varchar")]
            assert dbapi.NUMBER == description[0].type_code
            assert [column.type_code.arrow_type for column in description] == [pa.int64(), pa.string()]
            assert pickle.loads(pickle.dumps(description[0].type_code)).arrow_type == pa.int64()
            assert cur.arrow_schema.names == ["a", "s"]
            assert cur.rownumber == 0
            await cur.fetchone()
            await cur.fetchmany(2)
            assert cur.rownumber == 3
            assert (await cur.fetch_arrow_table()).equals(arrow_table(3, 2))
            assert cur.rownumber == 5


async def test_adbc_methods():
    async with FakeTrino() as trino:
        trino.pages[0].headers = {constants.HEADER_SET_CATALOG: "iceberg", constants.HEADER_SET_SCHEMA: "db"}
        trino.add_page(columns=COLUMNS, data=json_rows(0, 1))
        trino.add_page(columns=COLUMNS)
        async with trino.connect(encoding=None, catalog="tpch", schema="tiny") as conn:
            assert (conn.adbc_current_catalog, conn.adbc_current_db_schema) == ("tpch", "tiny")
            cur = await conn.cursor()
            await cur.execute("USE iceberg.db")
            assert (cur.adbc_current_catalog, cur.adbc_current_db_schema) == ("iceberg", "db")
            # the query is still running
            await cur.adbc_cancel()
            assert len(trino.recorded(method="DELETE")) == 1

            conn.adbc_current_catalog, conn.adbc_current_db_schema = "memory", "default"
            assert await cur.adbc_get_info() == {
                "vendor_name": "Trino", "vendor_version": "479", "vendor_sql": True, "vendor_substrait": False,
                "driver_name": "aiotrino", "driver_version": aiotrino.__version__, "driver_arrow_version": pa.__version__,
            }
            # sent with the headers of the session
            info_request = trino.recorded(prefix="/v1/info")[0]
            assert info_request.headers[constants.HEADER_USER] == "alice"
            assert info_request.headers[constants.HEADER_CATALOG] == "memory"
            assert info_request.headers[constants.HEADER_SCHEMA] == "default"


async def _fetch_fixture(method):
    async with FakeTrino() as trino:
        trino.add_page(columns=FIXTURE_COLUMNS, segments=[Segment(read_fixture(), 2)], encoding="arrow")
        async with trino.cursor("segment") as cur:
            await cur.execute("SELECT 1")
            return await method(cur)


async def test_fetch_df():
    pd = pytest.importorskip("pandas")
    df = await _fetch_fixture(lambda cur: cur.fetch_df())
    arrow_df = await _fetch_fixture(lambda cur: cur.fetch_df(arrow_dtypes=True))
    assert len(df) == 2
    assert df["c_varchar"][0] == "héllo"
    # TIME(9) values cannot be converted to Python times without losing the nanoseconds
    assert df["c_time9"].dtype == pd.ArrowDtype(pa.time64("ns"))
    assert all(isinstance(dtype, pd.ArrowDtype) for dtype in arrow_df.dtypes)


async def test_fetch_polars():
    pl = pytest.importorskip("polars")
    df = await _fetch_fixture(lambda cur: cur.fetch_polars())
    # polars has no year-month interval type: the number of months
    assert df["c_interval_ym"].to_list() == [14, None]

    mdn = pa.month_day_nano_interval()
    df = aiotrino.arrow.table_to_polars(pa.table({
        "l": pa.array([[(14, 0, 0), None], None], pa.list_(pa.field("element", mdn))),
        "m": pa.array([[("a", (3, 0, 0))], []], pa.map_(pa.string(), mdn)),
        "s": pa.array([{"x": (1, 0, 0)}, None], pa.struct([("x", mdn)])),
    }))
    assert df.schema["l"] == pl.List(pl.Int32)
    rows = df.to_dicts()
    assert [(row["l"], row["s"]) for row in rows] == [([14, None], {"x": 1}), (None, None)]
    # the representation of maps depends on the polars version
    assert rows[0]["m"] in ({"a": 3}, [{"key": "a", "value": 3}])
