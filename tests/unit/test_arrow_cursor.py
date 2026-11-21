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
"""Tests of the Arrow fetch methods of the cursors against a fake coordinator (see fake_trino.py)."""
import asyncio
from datetime import timedelta, timezone

import pytest

from aiotrino import constants
from aiotrino.exceptions import NotSupportedError, TrinoSegmentError
from aiotrino.types import NamedRowTuple
from tests.unit.arrow_fixtures import FIXTURE_COLUMNS, fixture_rows, read_fixture
from tests.unit.fake_trino import COLUMNS, FakeTrino, Segment, arrow_segment, arrow_table, json_rows, json_segment

pa = pytest.importorskip("pyarrow")

import aiotrino.arrow  # noqa: E402

SCHEMA = aiotrino.arrow.to_arrow_schema(COLUMNS)


def _values(table_or_batch):
    return table_or_batch.column("a").to_pylist()


@pytest.mark.parametrize("encoding", ["arrow+zstd", "json+lz4", None])
async def test_fetch_arrow_table(encoding):
    """The segment cursor returns the Arrow segments, the JSON segments and the rows of the direct protocol (server
    without spooling) as Arrow tables of the same schema."""
    async with FakeTrino() as trino:
        if encoding is None:
            trino.add_page(columns=COLUMNS, data=json_rows(0, 5))
            trino.add_page(columns=COLUMNS, data=json_rows(5, 4))
        else:
            make_segment = arrow_segment if encoding.startswith("arrow") else json_segment
            compression = encoding.split("+")[1]
            segments = [make_segment(0, 3, compression), make_segment(3, 2, compression, inline=True)]
            trino.add_page(columns=COLUMNS, segments=segments, encoding=encoding)
            trino.add_page(columns=COLUMNS, segments=[make_segment(5, 4, compression)], encoding=encoding)
        # with legacy_primitive_types, the rows are mapped to Python types for the conversion
        async with trino.cursor("segment", encoding=encoding or "arrow", legacy_primitive_types=encoding is None) as cur:
            await cur.execute("SELECT 1")
            table = await cur.fetch_arrow_table()
            assert table.equals(arrow_table(0, 9))
            assert table.schema == SCHEMA
            assert table.schema.field("s").metadata == {b"trino.type": b"varchar"}
            assert (await cur.fetch_arrow_table()).num_rows == 0
        assert sorted(trino.acks) == ([] if encoding is None else ["0", "2"])


@pytest.mark.parametrize(
    "cursor_style, page, schema",
    [
        ("segment", {"columns": COLUMNS, "segments": [], "encoding": "arrow"}, SCHEMA),
        ("row", {"columns": COLUMNS, "data": []}, SCHEMA),
        ("segment", {"update_type": "SET SESSION"}, pa.schema([])),
    ],
    ids=["no-segment", "no-row", "no-column"],
)
async def test_fetch_arrow_table_empty_result(cursor_style, page, schema):
    async with FakeTrino() as trino:
        trino.add_page(**page)
        trino.add_page(columns=page.get("columns"))
        async with trino.cursor(cursor_style) as cur:
            await cur.execute("SELECT 1")
            table = await cur.fetch_arrow_table()
    assert table.num_rows == 0
    assert table.schema == schema


async def test_segment_cursor_fetch_methods():
    """The fetch methods of the segment cursor can be mixed, each one continues with the rest of the result."""
    async with FakeTrino() as trino:
        trino.add_page(columns=COLUMNS, segments=[arrow_segment(0, 2), arrow_segment(2, 2)], encoding="arrow")
        segments = [arrow_segment(4, 2, delay=0.1), arrow_segment(6, 4, max_chunksize=2, delay=0.1)]
        trino.add_page(columns=COLUMNS, segments=segments, encoding="arrow")
        async with trino.cursor("segment") as cur:
            await cur.execute("SELECT 1")
            assert (await cur.fetchone()).encoding == "arrow"  # the first segment, as is
            assert cur.rownumber == 2
            assert _values(await cur.fetchone_arrow()) == [2, 3]
            # the iterator, left after its first batch, is closed by the event loop while the next calls run
            assert _values(await anext(cur.fetch_record_batches(prefetch=2))) == [4, 5]
            assert cur.rownumber == 6
            assert [_values(batch) async for batch in cur.fetch_record_batches()] == [[6, 7], [8, 9]]
            assert cur.rownumber == 10
            assert (await cur.fetchallarrow()).num_rows == 0


@pytest.mark.parametrize("legacy_primitive_types", [False, True])
async def test_row_cursor_fetch_arrow_table(legacy_primitive_types):
    """The row cursor converts the rest of the result (rows of the direct protocol and of the segments) to Arrow."""
    async with FakeTrino() as trino:
        trino.add_page(columns=COLUMNS, data=json_rows(0, 2))
        trino.add_page(columns=COLUMNS, segments=[json_segment(2, 2), json_segment(4, 2)], encoding="json")
        async with trino.cursor(encoding="json", legacy_primitive_types=legacy_primitive_types) as cur:
            await cur.execute("SELECT 1")
            assert await cur.fetchone() == [0, "v0"]
            batches = [batch async for batch in cur.fetch_record_batches()]
            assert pa.Table.from_batches(batches).equals(arrow_table(1, 5))
            assert (await cur.fetch_arrow_table()).num_rows == 0


@pytest.mark.parametrize("interruption", ["error", "cancel", "aclose"])
async def test_interrupted_fetch_cancels_the_downloads(interruption):
    """After an error, a cancellation or leaving fetch_record_batches(), no download keeps running in the background
    and the segments not delivered are loaded again by the next call."""
    first = {"error": {"fail_status": 500, "fail_times": 1}, "cancel": {"delay": 0.2}, "aclose": {}}[interruption]
    async with FakeTrino() as trino:
        segments = [arrow_segment(0, 2, **first)] + [arrow_segment(i, 2, delay=0.2) for i in (2, 4, 6)]
        trino.add_page(columns=COLUMNS, segments=segments, encoding="arrow")
        async with trino.cursor("segment") as cur:
            await cur.execute("SELECT 1")
            received = []
            if interruption == "aclose":
                batches = cur.fetch_record_batches(prefetch=4)
                received.append(await anext(batches))
                await batches.aclose()
            else:
                task = asyncio.create_task(cur.fetch_arrow_table())
                if interruption == "cancel":
                    await asyncio.sleep(0.05)
                    task.cancel()
                with pytest.raises(TrinoSegmentError if interruption == "error" else asyncio.CancelledError):
                    await task
            assert trino.segment_tasks() == []
            rest = await cur.fetch_arrow_table()
    assert pa.Table.from_batches(received + rest.to_batches()).equals(arrow_table(0, 8))


@pytest.mark.parametrize("method, max_downloads", [("fetch_arrow_table", 2), ("fetch_record_batches", 3)])
async def test_bounded_concurrent_downloads(monkeypatch, method, max_downloads):
    monkeypatch.setattr(constants, "MAX_PARALLEL_SEGMENT_RETRIEVAL", 2)
    async with FakeTrino() as trino:
        trino.add_page(columns=COLUMNS, segments=[arrow_segment(i, 1, delay=0.05) for i in range(6)], encoding="arrow")
        async with trino.cursor("segment") as cur:
            await cur.execute("SELECT 1")
            if method == "fetch_arrow_table":
                values = _values(await cur.fetch_arrow_table())
            else:
                values = [value async for batch in cur.fetch_record_batches(prefetch=3) for value in _values(batch)]
    assert values == list(range(6))
    assert trino.max_downloading == max_downloads


async def test_heartbeats_between_fetch_calls():
    """The segment cursor keeps the query alive while the caller is slow between two calls."""
    async with FakeTrino() as trino:
        # the next pages are fetched ahead: the query is still running after the first call
        for start in (0, 2, 4):
            trino.add_page(columns=COLUMNS, segments=[arrow_segment(start, 1), arrow_segment(start + 1, 1)], encoding="arrow")
        trino.add_page(columns=COLUMNS)
        async with trino.cursor("segment", heartbeat_interval=0.02) as cur:
            await cur.execute("SELECT 1")
            assert _values(await cur.fetchone_arrow()) == [0]
            await asyncio.sleep(0.2)
            assert len(trino.recorded(method="HEAD")) >= 2
            assert _values(await cur.fetch_arrow_table()) == [1, 2, 3, 4, 5]


async def test_row_cursor_decodes_arrow_segments_like_json():
    async with FakeTrino() as trino:
        segments = [Segment(read_fixture(), 2), Segment(read_fixture(), 2, inline=True)]
        trino.add_page(columns=FIXTURE_COLUMNS, segments=segments, encoding="arrow")
        async with trino.connect() as conn:
            cur = await conn.cursor()
            await cur.execute("SELECT 1")
            rows = await cur.fetchall()
            with pytest.raises(NotSupportedError, match="legacy_primitive_types"):
                await (await conn.cursor(legacy_primitive_types=True)).execute("SELECT 1")
    assert rows == fixture_rows() * 2
    by_name = dict(zip([column["name"] for column in FIXTURE_COLUMNS], rows[0], strict=True))
    # TIME WITH TIME ZONE / TIMESTAMP WITH TIME ZONE: the same instant as with the JSON encodings, but in UTC
    assert by_name["c_timetz3"].tzinfo == timezone.utc
    assert by_name["c_tstz3"].utcoffset() == timedelta(0)
    assert isinstance(by_name["c_row_anon"], NamedRowTuple)
