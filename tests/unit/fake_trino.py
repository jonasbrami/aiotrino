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
"""A fake Trino coordinator (on ``127.0.0.1``) and a fake object storage (on ``localhost``: another origin, like an S3
bucket), built on aiohttp's test server. ``pages`` are the responses to the statement POST and to the next GETs."""
from __future__ import annotations

import asyncio
import base64
import contextlib
import io
import json
from collections import namedtuple
from dataclasses import dataclass, field
from typing import Any, Optional

from aiohttp import web
from aiohttp.test_utils import TestServer
from multidict import CIMultiDict

import aiotrino.dbapi

COLUMNS = [
    {"name": "a", "type": "bigint", "typeSignature": {"rawType": "bigint", "arguments": []}},
    {"name": "s", "type": "varchar",
     "typeSignature": {"rawType": "varchar", "arguments": [{"kind": "LONG", "value": 2147483647}]}},
]
Recorded = namedtuple("Recorded", "server method path headers")


@dataclass
class Segment:
    data: bytes  # the encoded (possibly compressed) segment
    rows: int
    inline: bool = False
    uncompressed_size: Optional[int] = None
    headers: dict[str, list[str]] = field(default_factory=dict)  # to be sent by the client with the download
    delay: float = 0.0
    fail_status: Optional[int] = None
    fail_times: int = 1  # fail the first N downloads with fail_status
    reset_times: int = 0  # abort the connection in the middle of the body for the first N downloads
    id: str = field(default="", init=False)


@dataclass
class Page:
    columns: Optional[list[dict[str, Any]]] = None
    data: Optional[list[list[Any]]] = None  # rows of the direct protocol
    segments: Optional[list[Segment]] = None  # spooling protocol
    encoding: Optional[str] = None
    error: Optional[dict[str, Any]] = None
    update_type: Optional[str] = None
    update_count: Optional[int] = None
    headers: dict[str, str] = field(default_factory=dict)
    status: int = 200
    empty_body_times: int = 0
    reset_times: int = 0


class FakeTrino:
    def __init__(self, *, redirect: bool = False, heartbeat: bool = True, ack_delay: float = 0.0):
        # COORDINATOR_STORAGE_REDIRECT mode: segment URIs point to the coordinator which redirects to the storage
        self.redirect = redirect
        self.heartbeat = heartbeat
        self.ack_delay = ack_delay
        self.pages: list[Page] = [Page()]  # the response to the POST: the query is queued
        self.segments: dict[str, Segment] = {}
        self.requests: list[Recorded] = []
        self.attempts: dict[tuple[str, str], int] = {}  # number of requests of each page and segment
        self.downloading = self.max_downloading = 0
        self._deleted: set[str] = set()  # like the server, an acknowledged segment is deleted

    def add_page(self, **kwargs) -> Page:
        page = Page(**kwargs)
        for segment in page.segments or ():
            segment.id = str(len(self.segments))
            self.segments[segment.id] = segment
        self.pages.append(page)
        return page

    async def __aenter__(self) -> FakeTrino:
        coordinator = web.Application()
        coordinator.router.add_post("/v1/statement", self._statement)
        coordinator.router.add_route("*", "/v1/statement/executing/{query_id}/{slug}/{token}", self._statement)
        coordinator.router.add_get("/v1/spooled/download/{id}", self._redirect_to_storage)
        coordinator.router.add_get("/v1/spooled/ack/{id}", self._ack)
        coordinator.router.add_get("/v1/info", self._info)
        storage = web.Application()
        storage.router.add_get("/bucket/{id}", self._download)
        self._coordinator = TestServer(coordinator, host="127.0.0.1")
        self._storage = TestServer(storage, host="localhost")
        # the client sends large X-Trino-Session headers back, raise the server limits like Trino does
        await self._coordinator.start_server(max_line_size=1_048_576, max_field_size=1_048_576)
        await self._storage.start_server()
        self.port = self._coordinator.port
        return self

    async def __aexit__(self, *exc_info) -> None:
        await self._coordinator.close()
        await self._storage.close()

    def connect(self, **kwargs) -> aiotrino.dbapi.Connection:
        kwargs = {"host": "127.0.0.1", "user": "alice", "max_attempts": 1, "encoding": "arrow", **kwargs}
        return aiotrino.dbapi.connect(port=self.port, http_scheme="http", **kwargs)

    @contextlib.asynccontextmanager
    async def cursor(self, cursor_style: str = "row", **kwargs):
        async with self.connect(**kwargs) as conn:
            yield await conn.cursor(cursor_style)

    def recorded(self, server: str = "coordinator", method: Optional[str] = None, prefix: str = "") -> list[Recorded]:
        return [r for r in self.requests if r.server == server and method in (None, r.method) and r.path.startswith(prefix)]

    @property
    def acks(self) -> list[str]:
        return [r.path.rsplit("/", 1)[1] for r in self.recorded(prefix="/v1/spooled/ack/")]

    @staticmethod
    def segment_tasks() -> list[asyncio.Task]:  # the tasks of the client loading segments
        return [task for task in asyncio.all_tasks() if "_load_segment" in repr(task.get_coro())]

    def _record(self, server: str, request: web.Request) -> None:
        self.requests.append(Recorded(server, request.method, request.path, CIMultiDict(request.headers)))

    def _attempt(self, *key: str) -> int:  # how many times this resource was requested before
        self.attempts[key] = self.attempts.get(key, 0) + 1
        return self.attempts[key] - 1

    def _storage_uri(self, segment_id: str) -> str:
        return f"http://localhost:{self._storage.port}/bucket/{segment_id}?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Signature=abc"

    def _page_body(self, token: int, base_url: str) -> dict[str, Any]:
        page = self.pages[token]
        last = token == len(self.pages) - 1
        body: dict[str, Any] = {
            "id": "query",
            "infoUri": f"{base_url}/ui/query.html?query",
            "stats": {"state": "FAILED" if page.error else "FINISHED" if last else "RUNNING", "queued": token == 0},
        }
        if not last and page.error is None:
            body["nextUri"] = f"{base_url}/v1/statement/executing/query/slug/{token + 1}"
        for key, value in (("columns", page.columns), ("error", page.error), ("updateType", page.update_type),
                           ("updateCount", page.update_count), ("data", page.data)):
            if value is not None:
                body[key] = value
        if page.segments is not None:
            body["data"] = {"encoding": page.encoding, "segments": [self._segment_body(s, base_url) for s in page.segments]}
        return body

    def _segment_body(self, segment: Segment, base_url: str) -> dict[str, Any]:
        metadata: dict[str, Any] = {"segmentSize": len(segment.data), "rowOffset": 0, "rowsCount": segment.rows}
        if segment.uncompressed_size is not None:
            metadata["uncompressedSize"] = segment.uncompressed_size
        if segment.inline:
            return {"type": "inline", "data": base64.b64encode(segment.data).decode(), "metadata": metadata}
        return {
            "type": "spooled",
            "uri": f"{base_url}/v1/spooled/download/{segment.id}" if self.redirect else self._storage_uri(segment.id),
            "ackUri": f"{base_url}/v1/spooled/ack/{segment.id}",
            "headers": segment.headers,
            "metadata": metadata,
        }

    async def _statement(self, request: web.Request) -> web.StreamResponse:
        self._record("coordinator", request)
        await request.read()
        if request.method == "HEAD":
            return web.Response(status=200 if self.heartbeat else 405)
        if request.method == "DELETE":
            return web.Response(status=204)
        token = int(request.match_info.get("token", 0))
        page = self.pages[token]
        attempt = self._attempt("page", str(token))
        if page.status != 200:
            return web.Response(status=page.status, text="error")
        if attempt < page.empty_body_times:
            return web.Response(status=200, body=b"", headers=page.headers)
        payload = json.dumps(self._page_body(token, f"http://{request.host}")).encode()
        if attempt < page.reset_times:
            return await _reset_mid_body(request, payload)
        return web.Response(body=payload, content_type="application/json", headers=page.headers)

    async def _redirect_to_storage(self, request: web.Request) -> web.StreamResponse:
        self._record("coordinator", request)
        return web.Response(status=303, headers={"Location": self._storage_uri(request.match_info["id"])})

    async def _ack(self, request: web.Request) -> web.StreamResponse:
        self._record("coordinator", request)
        self._deleted.add(request.match_info["id"])
        await asyncio.sleep(self.ack_delay)
        return web.Response(status=200)

    async def _info(self, request: web.Request) -> web.StreamResponse:
        self._record("coordinator", request)
        return web.json_response({"nodeVersion": {"version": "479"}, "coordinator": True, "starting": False})

    async def _download(self, request: web.Request) -> web.StreamResponse:
        self._record("storage", request)
        segment = self.segments[request.match_info["id"]]
        if segment.id in self._deleted:
            return web.Response(status=404, text="NoSuchKey")
        if "Authorization" in request.headers:
            # S3 rejects presigned URLs combined with another authentication mechanism
            return web.Response(status=400, text="Only one auth mechanism allowed")
        attempt = self._attempt("segment", segment.id)
        self.downloading += 1
        self.max_downloading = max(self.max_downloading, self.downloading)
        try:
            await asyncio.sleep(segment.delay)
        finally:
            self.downloading -= 1
        if segment.fail_status is not None and attempt < segment.fail_times:
            return web.Response(status=segment.fail_status, text="storage error")
        if attempt < segment.reset_times:
            return await _reset_mid_body(request, segment.data)
        return web.Response(body=segment.data, content_type="application/octet-stream")


async def _reset_mid_body(request: web.Request, payload: bytes) -> web.StreamResponse:
    response = web.StreamResponse(status=200)
    response.content_length = len(payload)
    await response.prepare(request)
    await response.write(payload[: max(1, len(payload) // 2)])
    request.transport.close()
    return response


def json_rows(start: int, count: int) -> list[list[Any]]:
    """``count`` rows matching COLUMNS: ``a`` from ``start`` and ``s`` = ``f"v{a}"``."""
    return [[i, f"v{i}"] for i in range(start, start + count)]


def json_segment(start: int, count: int, compression: Optional[str] = None, **kwargs) -> Segment:
    raw = json.dumps(json_rows(start, count)).encode()
    if compression == "zstd":
        import zstandard
        return Segment(zstandard.ZstdCompressor().compress(raw), count, uncompressed_size=len(raw), **kwargs)
    if compression == "lz4":
        import lz4.block
        return Segment(lz4.block.compress(raw, store_size=False), count, uncompressed_size=len(raw), **kwargs)
    return Segment(raw, count, **kwargs)


def arrow_table(start: int, count: int):
    """The rows of :func:`json_rows` as an Arrow table."""
    import pyarrow as pa
    values = range(start, start + count)
    return pa.table({"a": pa.array(values, pa.int64()), "s": pa.array([f"v{i}" for i in values], pa.string())})


def arrow_segment(start: int, count: int, compression: Optional[str] = None, max_chunksize=None, **kwargs) -> Segment:
    import pyarrow.ipc as ipc
    sink = io.BytesIO()
    table = arrow_table(start, count)
    with ipc.new_stream(sink, table.schema, options=ipc.IpcWriteOptions(compression=compression)) as writer:
        writer.write_table(table, max_chunksize=max_chunksize)
    return Segment(sink.getvalue(), count, **kwargs)
