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
from datetime import timedelta

import pytest

from aiotrino.exceptions import TrinoDataError
from aiotrino.mapper import RowMapperFactory


def map_interval_day_to_second(value):
    columns = [{"typeSignature": {"rawType": "interval day to second", "arguments": []}}]
    return RowMapperFactory().create(columns=columns, legacy_primitive_types=False).map([[value]])[0][0]


@pytest.mark.parametrize(
    "value, expected",
    [
        (None, None),
        # milliseconds (server without the PARAMETRIC_INTERVAL client capability)
        ("1 02:03:04.123", timedelta(days=1, hours=2, minutes=3, seconds=4, milliseconds=123)),
        ("-0 00:00:01.500", -timedelta(seconds=1, milliseconds=500)),
        # parametric intervals: 0 to 12 fractional digits
        ("-1 02:03:04", -timedelta(days=1, hours=2, minutes=3, seconds=4)),
        ("0 00:00:01.05", timedelta(seconds=1, milliseconds=50)),
        ("1 02:03:04.123456", timedelta(days=1, hours=2, minutes=3, seconds=4, microseconds=123456)),
        # rounded half to even to microseconds
        ("0 00:00:00.1234565", timedelta(microseconds=123456)),
        ("0 00:00:00.1234575", timedelta(microseconds=123458)),
        ("0 23:59:59.9999995", timedelta(days=1)),
        ("106751991 04:00:54.775", timedelta(days=106751991, hours=4, seconds=54, milliseconds=775)),
    ],
)
def test_interval_day_to_second(value, expected):
    assert map_interval_day_to_second(value) == expected


@pytest.mark.parametrize("value", ["1 02:03", "1 02:03:04.1234567890123", "x 00:00:00", "1000000000 00:00:00.000"])
def test_invalid_interval_day_to_second(value):
    with pytest.raises(TrinoDataError):
        map_interval_day_to_second(value)
