"""HTTP client for the renewables data API."""

import datetime
import logging
from io import StringIO
from types import TracebackType
from typing import Optional, Type

import httpx
import pandas as pd
from tenacity import before_sleep_log, retry, stop_after_attempt, wait_exponential

from .settings import Settings, settings

logger = logging.getLogger(__name__)


class ApiClientError(Exception):
    """Raised when the API returns an unexpected response."""


class ApiClient:
    
    def __init__(self, cfg: Settings = settings) -> None:
        self._base_url = cfg.DATA_SOURCE_BASE_URL.rstrip("/")
        self._api_key = cfg.API_KEY
        self._client = httpx.AsyncClient(timeout=cfg.HTTP_TIMEOUT)
        self._retry_cfg = dict(
            stop=stop_after_attempt(cfg.HTTP_MAX_RETRIES),
            wait=wait_exponential(
                min=cfg.HTTP_RETRY_MIN_WAIT,
                max=cfg.HTTP_RETRY_MAX_WAIT,
            ),
            before_sleep=before_sleep_log(logger, logging.WARNING),
            reraise=True,
        )

   
    async def __aenter__(self) -> "ApiClient":
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        await self.close()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _request(
        self, endpoint: str, requested_date: datetime.date
    ) -> httpx.Response:
        
        @retry(**self._retry_cfg)
        async def _get() -> httpx.Response:
            url = f"{self._base_url}/{requested_date.isoformat()}/{endpoint}"
            logger.debug("GET %s", url)
            response = await self._client.get(
                url, params={"api_key": self._api_key}
            )
            response.raise_for_status()
            return response

        return await _get()

    @staticmethod
    def _parse_response(response: httpx.Response) -> pd.DataFrame:
        content_type = response.headers.get("content-type", "")

        if "application/json" in content_type:
            return pd.DataFrame(response.json())

        if "text/csv" in content_type:
            return pd.read_csv(StringIO(response.text))

        raise ApiClientError(
            f"Unsupported content-type '{content_type}' from {response.url}"
        )

    
    async def get_data(
        self, endpoint: str, requested_date: datetime.date
    ) -> pd.DataFrame:
    
        response = await self._request(endpoint, requested_date)
        df = self._parse_response(response)
        logger.debug(
            "Fetched %d rows from '%s' for %s",
            len(df),
            endpoint,
            requested_date,
        )
        return df

    async def close(self) -> None:
        await self._client.aclose()