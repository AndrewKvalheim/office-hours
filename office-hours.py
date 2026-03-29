#!/usr/bin/env -S uv run --script
#
# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "icalendar>=7.0.3",
#     "more-itertools>=10.8.0",
#     "pydantic>=2.12.5",
#     "pydash>=8.0.6",
#     "requests>=2.33.0",
#     "requests-cache>=1.3.1",
#     "requests-ratelimiter>=0.9.2",
#     "sunrisesunset>=1.0.2",
#     "xdg-base-dirs>=6.0.2",
# ]
# ///

import argparse
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from icalendar import Calendar, Event
from itertools import chain, pairwise
from logging import basicConfig as configLogging, getLogger
from more_itertools import split_when
from operator import itemgetter
from os import environ
from pathlib import Path
from pydantic import BaseModel, Field, field_validator
from pydash.collections import find
from pydash.functions import spread
from pydash.strings import pascal_case
from requests import Session
from requests_cache import CacheMixin
from requests_ratelimiter import LimiterMixin
from sunrisesunset import SunriseSunset
from typing import Self
from urllib.parse import quote as urlescape, urlencode
from xdg_base_dirs import xdg_cache_home
from zoneinfo import ZoneInfo

#
# Utilities
#


class CachedLimiterSession(CacheMixin, LimiterMixin, Session):
    """
    Web session with caching and rate limiting
    """


def duration_to_hms(duration: timedelta) -> str:
    """
    Format a duration as e.g. `1 h 30 m`.
    """
    h, m_s = divmod(int(duration.total_seconds()), 3600)
    m, s = divmod(m_s, 60)

    return " ".join([f"{n} {u}" for u, n in {"h": h, "m": m, "s": s}.items() if n])


def time_chunks(
    limit: timedelta, beginning: datetime, end: datetime
) -> Iterator[tuple[datetime, datetime]]:
    """
    Break a time range into contiguous time ranges of a limited size.
    """
    chunk_beginning = beginning
    while chunk_beginning < end:
        chunk_end = min(chunk_beginning + limit, end)
        yield (chunk_beginning, chunk_end)
        chunk_beginning = chunk_end


#
# Hard-coded parameters
#

API_ENDPOINT = "https://api.tidesandcurrents.noaa.gov/api/prod/datagetter"
API_INTERVAL = timedelta(minutes=5)
API_LIMIT = timedelta(days=30)
APP_HOMEPAGE = "https://github.com/AndrewKvalheim/office-hours"
APP_NAME = "office-hours"
APP_NAMESPACE = "office-hours.andrew.kvalhe.im"
APP_VERSION = "1.0.0"
OK_DURATION_MIN = timedelta(hours=1)
OK_HEIGHT_MIN = 1.0  # m
OK_RATE_MIN = 0.2  # m/h
TZ_ABBR_TO_ID = {"PDT": "America/Los_Angeles", "PST": "America/Los_Angeles"}

#
# Global objects
#

logger = getLogger(APP_NAME)
debug, info, warn = logger.debug, logger.info, logger.warning

cache_dir = xdg_cache_home() / APP_NAME
session = CachedLimiterSession(cache_name=cache_dir / "api-cache", per_second=1)
session.headers["User-Agent"] = f"{APP_NAME}/{APP_VERSION} ({APP_HOMEPAGE})"

#
# Application
#


class Tide(BaseModel, frozen=True):
    height: float = Field(alias="v")
    time: datetime = Field(alias="t")

    @field_validator("time", mode="before")
    @classmethod
    def parse_time(cls, t: str, info) -> datetime:
        dt = datetime.strptime(t, "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)
        return dt.astimezone(info.context["tz"])


@dataclass(frozen=True)
class Station:
    id: str
    latitude: float
    longitude: float
    name: str
    tz: ZoneInfo

    @classmethod
    def from_id(cls, id: str) -> Self:
        url = f"https://api.tidesandcurrents.noaa.gov/mdapi/prod/webapi/stations/{urlescape(id)}.json"
        response = session.get(url)
        response.raise_for_status()
        data = find(response.json()["stations"], lambda s: s["id"] == id)
        assert data

        return cls(
            id=id,
            name=data["name"],
            latitude=data["lat"],
            longitude=data["lng"],
            tz=ZoneInfo(TZ_ABBR_TO_ID[data["timezone"]]),
        )

    def daylight(self, day_of: datetime) -> tuple[datetime, datetime]:
        return SunriseSunset(
            day_of, lat=self.latitude, lon=self.longitude, zenith="civil"
        ).sun_rise_set

    def get_tides(self, since: datetime, until: datetime) -> Iterator[Tide]:
        class Response(BaseModel):
            predictions: list[Tide]

        def format_dt(dt: datetime) -> str:
            return dt.astimezone(timezone.utc).strftime("%Y%m%d %H:%M")

        for beginning, end in time_chunks(API_LIMIT, since, until):
            params = {
                "application": pascal_case(APP_NAME),
                "format": "json",
                "units": "metric",
                "time_zone": "gmt",
                "product": "predictions",
                "datum": "MLLW",
                "interval": int(API_INTERVAL / timedelta(minutes=1)),
                "station": self.id,
                "begin_date": format_dt(beginning),
                "end_date": format_dt(end),
            }

            response = session.get(API_ENDPOINT, params=params)
            response.raise_for_status()
            data = Response.model_validate_json(response.text, context={"tz": self.tz})

            yield from data.predictions

    def plot_url(self, first_date: date, last_date: date) -> str:
        params = {
            "id": self.id,
            "units": "metric",
            "bdate": first_date.strftime("%Y%m%d"),
            "edate": last_date.strftime("%Y%m%d"),
            "timezone": "LST/LDT",
            "clock": "24hour",
            "datum": "MLLW",
            "interval": "hilo",
            "action": "dailychart",
        }

        return f"https://tidesandcurrents.noaa.gov/noaatidepredictions.html?{urlencode(params)}"


def tides_to_events(station: Station, tides: Iterable[Tide]) -> Iterator[Event]:
    criteria = [
        f"height ≥ {OK_HEIGHT_MIN} m",
        f"rate ≥ {OK_RATE_MIN} m/h",
        f"duration ≥ {duration_to_hms(OK_DURATION_MIN)}",
        "time sunrise–sunset (civil)",
    ]
    interval_h = API_INTERVAL / timedelta(hours=1)

    def is_block_ok(first: Tide, last: Tide) -> bool:
        return last.time - first.time >= OK_DURATION_MIN

    def is_tide_ok(previous: Tide, tide: Tide) -> bool:
        dawn, dusk = station.daylight(tide.time)

        return (
            tide.height >= OK_HEIGHT_MIN
            and (tide.height - previous.height) / interval_h >= OK_RATE_MIN
            and dawn <= tide.time <= dusk
        )

    def is_tide_separate(previous: Tide, tide: Tide) -> bool:
        return tide.time - previous.time > API_INTERVAL

    tides_ok = map(itemgetter(1), filter(spread(is_tide_ok), pairwise(tides)))
    blocks = map(itemgetter(0, -1), split_when(tides_ok, is_tide_separate))
    blocks_ok = filter(spread(is_block_ok), blocks)

    for first, last in blocks_ok:
        block_id = last.time.astimezone(timezone.utc).strftime("%Y%m%d%H")
        plot_url = station.plot_url(first.time.date(), last.time.date())
        description = {
            "Tide range": f"{first.height:.1f}–{last.height:.1f} m",
            "Tide plot": plot_url,
            "Criteria": ", ".join(criteria),
        }

        yield Event.new(
            uid=f"v1-{station.id}-{block_id}@{APP_NAMESPACE}",
            summary="Office hours",
            location=station.name,
            start=first.time,
            end=last.time,
            description="\n".join([f"{k}: {v}" for k, v in description.items()]),
        )


def main() -> None:
    configLogging(level=environ.get("LOG_LEVEL", "INFO"))

    # Interpret parameters
    parser = argparse.ArgumentParser()
    parser.add_argument("station", help="NOAA station ID", type=Station.from_id)
    parser.add_argument("path", help="iCalendar file path", type=Path)
    parser.add_argument(
        "--horizon",
        help="Calendar horizon in days from today (Default: 30)",
        type=lambda n: timedelta(days=int(n)),
        default="30",
    )
    args = parser.parse_args()
    horizon, path, station = args.horizon, args.path, args.station

    # Load existing events
    existing = Calendar.from_ical(path).events if path.exists() else []
    if existing:
        beginning = existing[0].start.isoformat(timespec="minutes")
        end = existing[-1].end.isoformat(timespec="minutes")
        info(f"Existing calendar at {path} covers {beginning} to {end}")
    else:
        info(f"No existing calendar at {path}")

    # Retrieve tide predictions
    now = datetime.now().astimezone(station.tz)
    since = existing[-1].end if existing else station.daylight(now)[0]
    until = station.daylight(now + horizon)[1]
    if since < until:
        info(f"Extending to {until.isoformat(timespec='minutes')}")
    else:
        info(f"Already covers {until.isoformat(timespec='minutes')}")
        return
    tides = station.get_tides(since, until)

    # Generate calendar
    events = tides_to_events(station, tides)
    calendar = Calendar.new(
        uid=f"{APP_NAME}@{APP_NAMESPACE}",
        name="Office hours",
        subcomponents=chain(existing, events),
    )
    path.write_bytes(calendar.to_ical())
    info(f"Added {len(calendar.events) - len(existing)} calendar events")


if __name__ == "__main__":
    main()
