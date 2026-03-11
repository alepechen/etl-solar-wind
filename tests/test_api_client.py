# tests/test_api_client.py
import datetime
import pytest
import pandas as pd
from unittest.mock import AsyncMock, MagicMock
import httpx

from api_data_source.etl.api_client import ApiClient
from api_data_source.etl.etl_runner import ENDPOINTS

START_DATE = datetime.date(2024, 3, 15)

JSON_PAYLOAD = [
    {"ts": "2024-03-15T00:00:00Z", "variable": 1, "value": 99.5, "last_modified": "2024-03-15T00:00:00Z"},
    {"ts": "2024-03-15T01:00:00Z", "variable": 2, "value": 101.2, "last_modified": "2024-03-15T01:00:00Z"},
]


class TestApiClient:

    @pytest.fixture
    def client(self):
        return ApiClient()

    @pytest.fixture
    def start_date(self):
        return START_DATE

    def _make_response(self, content_type: str, payload) -> MagicMock:
        """Helper to build a fake httpx.Response."""
        mock = MagicMock(spec=httpx.Response)
        mock.headers = {"content-type": content_type}
        mock.json.return_value = payload
        mock.text = ""
        mock.raise_for_status = MagicMock() 
        return mock

    @pytest.mark.asyncio
    @pytest.mark.parametrize("endpoint", ENDPOINTS)
    async def test_get_data_returns_dataframe(self, client, endpoint, start_date):
        """get_data should return a populated DataFrame for each endpoint."""
        fake_response = self._make_response("application/json", JSON_PAYLOAD)
        client._client.get = AsyncMock(return_value=fake_response)

        result = await client.get_data(endpoint, start_date)

        assert isinstance(result, pd.DataFrame)
        assert len(result) == len(JSON_PAYLOAD)

    
    @pytest.mark.asyncio
    async def test_throttle_raises_after_max_retries(self, client, start_date):
        """get_data should raise after exhausting retries on 429 responses."""
        throttle_response = MagicMock(spec=httpx.Response)
        throttle_response.status_code = 429
        throttle_response.raise_for_status.side_effect = httpx.HTTPStatusError(
            "429 Too Many Requests",
            request=MagicMock(),
            response=throttle_response,
        )
        client._client.get = AsyncMock(return_value=throttle_response)

        with pytest.raises(httpx.HTTPStatusError):
            await client.get_data("renewables/windgen.csv", start_date)

        # Confirm it actually retried (called get 3 times, not just once)
        assert client._client.get.call_count == 3