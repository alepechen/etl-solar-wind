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
    """Async HTTP client with automatic retries and content-type negotiation.

    Intended to be used as an async context manager::

        async with ApiClient(settings) as client:
            df = await client.get_data("renewables/windgen.csv", date.today())

    Args:
        cfg: Application settings. Defaults to the module-level singleton.
    """

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

    # ------------------------------------------------------------------
    # Context-manager support
    # ------------------------------------------------------------------

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
        """Send a GET request, retrying on transient failures.

        Args:
            endpoint: Path segment appended after the date, e.g.
                ``"renewables/windgen.csv"``.
            requested_date: The date for which data is requested.

        Returns:
            The raw :class:`httpx.Response`.

        Raises:
            httpx.HTTPStatusError: On 4xx/5xx responses after all retries.
        """

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
        """Deserialise a response into a :class:`~pandas.DataFrame`.

        Args:
            response: A successful HTTP response whose content-type is either
                ``application/json`` or ``text/csv``.

        Returns:
            Parsed DataFrame.

        Raises:
            ApiClientError: If the content-type is not supported.
        """
        content_type = response.headers.get("content-type", "")

        if "application/json" in content_type:
            return pd.DataFrame(response.json())

        if "text/csv" in content_type:
            return pd.read_csv(StringIO(response.text))

        raise ApiClientError(
            f"Unsupported content-type '{content_type}' from {response.url}"
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def get_data(
        self, endpoint: str, requested_date: datetime.date
    ) -> pd.DataFrame:
        """Fetch and parse data for a single endpoint and date.

        Args:
            endpoint: API path segment, e.g. ``"renewables/windgen.csv"``.
            requested_date: Calendar date to request.

        Returns:
            DataFrame containing the raw API response data.
        """
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
        """Release underlying connection resources."""
        await self._client.aclose()