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
"""Tests of the spooling protocol (segment download, acknowledgement, decoding) against a fake coordinator."""
import asyncio
import threading

import pytest

import aiotrino.client
import aiotrino.dbapi
from aiotrino.auth import BasicAuthentication, JWTAuthentication
from aiotrino.exceptions import TrinoSegmentError
from tests.unit.fake_trino import COLUMNS, FakeTrino, Segment, json_rows, json_segment

SSE_C_HEADER = "x-amz-server-side-encryption-customer-key"


@pytest.mark.parametrize(
    "auth, redirect",
    [(BasicAuthentication("alice", "s3cret"), False), (JWTAuthentication("token"), True)],
    ids=["basic", "jwt-redirect"],
)
async def test_storage_only_gets_the_segment_headers(auth, redirect):
    """Credentials, X-Trino headers and custom headers never reach the object storage."""
    headers = {SSE_C_HEADER: ["sse-key"], "X-Multi": ["a", "b"]}
    async with FakeTrino(redirect=redirect) as trino:
        segments = [json_segment(0, 2, headers=headers), json_segment(2, 2, headers=headers)]
        trino.add_page(columns=COLUMNS, segments=segments, encoding="json")
        conn = trino.connect(encoding="json", auth=auth, http_headers={"X-Custom": "custom"},
                             extra_credential=[("credential.key", "secret")], session_properties={"timeout": "1h"})
        async with conn:
            cur = await conn.cursor()
            await cur.execute("SELECT 1")
            assert await cur.fetchall() == json_rows(0, 4)
        assert conn._segment_http_session.closed

    def leaked(request, *names):
        return [name for name in request.headers if name.lower() in names or name.lower().startswith("x-trino-")]

    assert len(trino.recorded("storage")) == 2
    for request in trino.recorded("storage"):
        assert leaked(request, "authorization", "x-custom") == []
        assert request.headers[SSE_C_HEADER] == "sse-key"
        assert request.headers.getall("X-Multi") == ["a", "b"]
    # the spooling endpoints of the coordinator get the custom headers, but no credentials
    spooling_requests = trino.recorded(method="GET", prefix="/v1/spooled/")
    assert len(spooling_requests) == (4 if redirect else 2)
    for request in spooling_requests:
        assert leaked(request, "authorization") == []
        assert request.headers["X-Custom"] == "custom"
    assert sorted(trino.acks) == ["0", "1"]
    statement = trino.recorded(method="POST")[0]
    assert "Authorization" in statement.headers
    assert statement.headers["X-Trino-Extra-Credential"] == "credential.key=secret"


async def test_segments_are_loaded_lazily_and_in_order():
    async with FakeTrino() as trino:
        # the first segment is the slowest to download
        segments = [json_segment(0, 2, delay=0.2), json_segment(2, 2), json_segment(4, 2), json_segment(6, 2)]
        trino.add_page(columns=COLUMNS, segments=segments, encoding="json")
        async with trino.cursor(encoding="json") as cur:
            await cur.execute("SELECT 1")
            # execute() loads the first segment (to know whether the result is empty) and prefetches the next ones
            assert "/bucket/3" not in [r.path for r in trino.recorded("storage")]
            assert await cur.fetchmany(3) == json_rows(0, 3)
            assert await cur.fetchall() == json_rows(3, 5)
        assert sorted(trino.acks) == ["0", "1", "2", "3"]


async def test_failed_segment_download_is_retried_then_loaded_again():
    async with FakeTrino() as trino:
        # a connection reset in the middle of the body, HTTP 503 (retried), then HTTP 500 (not retried)
        segments = [json_segment(0, 2, reset_times=1), json_segment(2, 2, fail_status=503, fail_times=1),
                    json_segment(4, 2, fail_status=500, fail_times=1)]
        trino.add_page(columns=COLUMNS, segments=segments, encoding="json")
        async with trino.cursor(encoding="json", max_attempts=2) as cur:
            await cur.execute("SELECT 1")
            assert await cur.fetchmany(4) == json_rows(0, 4)
            with pytest.raises(TrinoSegmentError, match="500") as error:
                await cur.fetchone()
            # the query string of a pre-signed URI is a credential
            assert "X-Amz-Signature" not in str(error.value)
            assert "2" not in trino.acks
            # the segment is not skipped
            assert await cur.fetchall() == json_rows(4, 2)
        assert sorted(r.path for r in trino.recorded("storage")) == ["/bucket/0", "/bucket/0", "/bucket/1", "/bucket/1",
                                                                     "/bucket/2", "/bucket/2"]
        assert sorted(trino.acks) == ["0", "1", "2"]


@pytest.mark.parametrize(
    "segment, message",
    [
        (Segment(b'[[0, "v0"', 1), "Failed to decode"),
        (Segment(b'[[0, "v0"], [1, "v1"]]', 3), "should contain 3 rows but 2 rows were decoded"),
    ],
)
async def test_invalid_segment(segment, message):
    async with FakeTrino() as trino:
        trino.add_page(columns=COLUMNS, segments=[segment], encoding="json")
        async with trino.cursor(encoding="json") as cur:
            with pytest.raises(TrinoSegmentError, match=message):
                await cur.execute("SELECT 1")
        assert trino.acks == []


def test_unavailable_codecs_are_not_requested(monkeypatch):
    monkeypatch.setattr(aiotrino.client, "CODECS_UNAVAILABLE", {"zstd": "No module named 'zstandard'"})
    encodings = aiotrino.dbapi.connect("localhost")._client_session.encoding
    assert [encoding for encoding in encodings if encoding.startswith("json")] == ["json+lz4", "json"]
    with pytest.raises(ValueError, match="zstd is not installed"):
        aiotrino.client.CompressedQueryDataDecoderFactory(None).create("json+zstd")


@pytest.mark.parametrize("compression", ["zstd", "lz4"])
async def test_compressed_segments_are_decoded_in_worker_threads(monkeypatch, compression):
    threads = []
    decode = aiotrino.client.JsonQueryDataDecoder.decode
    monkeypatch.setattr(aiotrino.client.JsonQueryDataDecoder, "decode",
                        lambda *args: threads.append(threading.current_thread()) or decode(*args))
    encoding = f"json+{compression}"
    async with FakeTrino() as trino:
        segments = [json_segment(0, 2, compression), json_segment(2, 2, compression, inline=True)]
        trino.add_page(columns=COLUMNS, segments=segments, encoding=encoding)
        async with trino.cursor(encoding=encoding) as cur:
            await cur.execute("SELECT 1")
            assert await cur.fetchall() == json_rows(0, 4)
        assert trino.acks == ["0"]
    assert len(threads) == 2
    assert threading.main_thread() not in threads


async def test_closing_the_cursor_cancels_the_pending_downloads():
    async with FakeTrino() as trino:
        trino.add_page(columns=COLUMNS, segments=[json_segment(0, 2), json_segment(2, 2, delay=5)], encoding="json")
        async with trino.cursor(encoding="json") as cur:
            await cur.execute("SELECT 1")
            assert await cur.fetchmany(2) == json_rows(0, 2)
            await asyncio.wait_for(cur.close(), 1)
            assert trino.segment_tasks() == []
        assert trino.acks == ["0"]


@pytest.mark.parametrize(
    "heartbeat_interval, server_supports_heartbeats, expected_targets",
    [
        # sent while the first segment is downloaded (nextUri /2), then while the consumer is slow (nextUri /3)
        (0.02, True, {"2", "3"}),
        # the server answered 405 to the first one
        (0.02, False, {"2"}),
        (None, True, set()),
    ],
)
async def test_heartbeats(heartbeat_interval, server_supports_heartbeats, expected_targets):
    async with FakeTrino(heartbeat=server_supports_heartbeats) as trino:
        trino.add_page(columns=COLUMNS, segments=[json_segment(0, 3, delay=0.1)], encoding="json")
        # the next page is fetched before the rows of the current one are served: the query is still running
        trino.add_page(columns=COLUMNS)
        trino.add_page(columns=COLUMNS)
        async with trino.cursor(encoding="json", heartbeat_interval=heartbeat_interval) as cur:
            await cur.execute("SELECT 1")
            while await cur.fetchone() is not None:
                await asyncio.sleep(0.04)  # the coordinator does not hear from the client
    targets = [r.path.rsplit("/", 1)[1] for r in trino.recorded(method="HEAD")]
    assert set(targets) == expected_targets
    if not server_supports_heartbeats:
        assert len(targets) == 1
