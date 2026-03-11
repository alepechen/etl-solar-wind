import asyncio
import datetime
import logging
from typing import Optional

import pandas as pd

from .api_client import ApiClient
from .data_handler import DataHandler
from .loader import load
from .settings import OutputFormat, Settings, settings

logger = logging.getLogger(__name__)

WIND_ENDPOINT = "renewables/windgen.csv"
SOLAR_ENDPOINT = "renewables/solargen.json"

ENDPOINTS: list[str] = [WIND_ENDPOINT, SOLAR_ENDPOINT]


def _resolve_date(date_str: Optional[str]) -> datetime.date:
    if date_str is None:
        return datetime.date.today()
    try:
        return datetime.date.fromisoformat(date_str)
    except ValueError as exc:
        raise ValueError(
            f"Invalid date '{date_str}'. Expected format: YYYY-MM-DD."
        ) from exc


def _last_week_dates(anchor: datetime.date) -> list[datetime.date]:
    return [anchor - datetime.timedelta(days=i) for i in range(7)]


async def _fetch_all(
    client: ApiClient,
    endpoints: list[str],
    dates: list[datetime.date],
) -> dict[str, list[pd.DataFrame]]:

    frames: dict[str, list[pd.DataFrame]] = {ep: [] for ep in endpoints}

    async def _fetch(endpoint: str, date: datetime.date) -> None:
        logger.debug("Fetching endpoint='%s' date=%s", endpoint, date)
        df = await client.get_data(endpoint, date)
        frames[endpoint].append(df)

    tasks = [
        _fetch(endpoint, date)
        for date in dates
        for endpoint in endpoints
    ]
    await asyncio.gather(*tasks)
    return frames


async def run_pipeline(
    date_str: Optional[str] = None,
    output_format: OutputFormat = OutputFormat.CSV,
    cfg: Settings = settings,
) -> None:
    anchor = _resolve_date(date_str)
    dates = _last_week_dates(anchor)
    handler = DataHandler()

    logger.info(
        "Starting pipeline | anchor=%s | dates=%s…%s | format=%s",
        anchor,
        dates[-1],
        dates[0],
        output_format.value,
    )

    async with ApiClient(cfg) as client:
        frames = await _fetch_all(client, ENDPOINTS, dates)

    for endpoint, dfs in frames.items():
        if not dfs:
            logger.warning("No data returned for endpoint '%s'. Skipping.", endpoint)
            continue

        combined = pd.concat(dfs, ignore_index=True)
        transformed = handler.clean_and_transform(combined)

        dataset_name = endpoint.split("/")[-1].split(".")[0] + "_generation"
        load(transformed, dataset_name, fmt=output_format, cfg=cfg)
        logger.info(
            "Loaded %d rows for '%s'.", len(transformed), dataset_name
        )

    logger.info("Pipeline complete.")