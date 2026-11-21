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
"""Tests of the HTTP behaviour of the client against a fake coordinator (see fake_trino.py)."""
import asyncio

import aiohttp
import pytest

import aiotrino.dbapi
from aiotrino import constants, exceptions
from tests.unit.fake_trino import COLUMNS, FakeTrino, Page, json_rows


async def test_session_state_is_sent_with_the_next_requests():
    """The response headers update the client session, the Trino headers of each request are built from it."""
    async with FakeTrino() as trino:
        async with trino.connect(encoding=None) as conn:
            # a cursor created before the changes
            cur = await conn.cursor()

            async def execute(response_headers):
                trino.pages[0].headers = response_headers
                await cur.execute("SELECT 1")
                await cur.fetchall()
                return trino.recorded(method="POST")[-1].headers

            # aiohttp limits header fields to 8 KiB by default, Trino can send much larger session headers
            large_value = "x" * 20_000
            await execute({constants.HEADER_SET_SESSION: f"large_property={large_value}",
                           constants.HEADER_ADDED_PREPARE: "st1=SELECT+1", constants.HEADER_SET_AUTHORIZATION_USER: "bob"})
            assert conn._client_session.properties == {"large_property": large_value}
            headers = await execute({constants.HEADER_DEALLOCATED_PREPARE: "st1"})
            assert headers[constants.HEADER_SESSION] == f"large_property={large_value}"
            assert headers[constants.HEADER_PREPARED_STATEMENT] == "st1=SELECT+1"
            assert (headers[constants.HEADER_USER], headers[constants.HEADER_ORIGINAL_USER]) == ("bob", "alice")
            headers = await execute({constants.HEADER_RESET_AUTHORIZATION_USER: "true"})
            assert constants.HEADER_PREPARED_STATEMENT not in headers
            headers = await execute({})
            assert headers[constants.HEADER_USER] == "alice"
            assert constants.HEADER_ORIGINAL_USER not in headers
            assert constants.HEADER_USER not in conn._http_session.headers


def test_http_session_is_created_lazily_and_only_closed_if_owned():
    # created before the event loop runs: the connection creates its HTTP session on first use
    owned = aiotrino.dbapi.connect(host="127.0.0.1", port=1, user="alice", encoding=None)

    async def main():
        async with FakeTrino() as trino, aiohttp.ClientSession() as user_session:
            trino.add_page(columns=COLUMNS, data=json_rows(0, 1))
            owned.port = trino.port
            for conn in (owned, trino.connect(encoding=None, http_session=user_session)):
                cur = await conn.cursor()
                await cur.execute("SELECT 1")
                assert await cur.fetchall() == json_rows(0, 1)
                session = conn._http_session
                await conn.close()
                assert conn.is_closed()
                with pytest.raises(exceptions.InterfaceError):
                    await conn.cursor()
                assert session.closed == (conn is owned)
            assert not user_session.headers

    asyncio.run(main())


@pytest.mark.parametrize(
    "failure, max_attempts, error",
    [
        ({"reset_times": 1}, 2, None),  # connection reset in the middle of the body
        ({"empty_body_times": 1}, 2, None),
        ({"reset_times": 1}, 1, exceptions.OperationalError),
        ({"empty_body_times": 2}, 2, exceptions.TrinoConnectionError),
    ],
    ids=["reset", "empty-body", "reset-without-retry", "empty-body-after-retries"],
)
async def test_statement_response_is_retried(failure, max_attempts, error):
    async with FakeTrino() as trino:
        trino.add_page(columns=COLUMNS, data=json_rows(0, 1), **failure)
        trino.add_page(columns=COLUMNS, data=json_rows(1, 1))
        async with trino.cursor(encoding=None, max_attempts=max_attempts) as cur:
            if error is None:
                await cur.execute("SELECT 1")
                assert await cur.fetchall() == json_rows(0, 2)
            else:
                with pytest.raises(error):
                    await cur.execute("SELECT 1")
        tokens = [r.path.rsplit("/", 1)[1] for r in trino.recorded(method="GET")]
        assert tokens == ["1"] * max_attempts + (["2"] if error is None else [])


@pytest.mark.parametrize("update_type, cancelled", [("INSERT", False), (None, True)], ids=["dml", "select"])
async def test_closing_the_cursor_only_cancels_a_running_query(update_type, cancelled):
    """execute() reads the result of a DML statement to the end: the completed statement must not be cancelled."""
    async with FakeTrino() as trino:
        update_count = 3 if update_type else None
        for data in ([[3, "3"]], [[3, "3"]], None):
            trino.add_page(columns=COLUMNS, data=data, update_type=update_type, update_count=update_count)
        async with trino.cursor(encoding=None) as cur:
            await cur.execute("INSERT INTO t VALUES (1), (2), (3)")
            assert cur.rowcount == (update_count or -1)
            await cur.close()
            assert len(trino.recorded(method="DELETE")) == cancelled


@pytest.mark.parametrize(
    "page, error_class, vendor_code",
    [
        (Page(error={"message": "boom", "errorCode": 65536, "errorName": "GENERIC_INTERNAL_ERROR",
                     "errorType": "INTERNAL_ERROR"}), exceptions.InternalError, 65536),
        (Page(status=503), exceptions.OperationalError, None),
    ],
    ids=["query-error", "http-503"],
)
async def test_errors_are_dbapi_errors(page, error_class, vendor_code):
    async with FakeTrino() as trino:
        trino.pages.append(page)
        async with trino.cursor(encoding=None) as cur:
            with pytest.raises(error_class) as error:
                await cur.execute("SELECT 1")
    assert getattr(error.value, "vendor_code", None) == vendor_code
