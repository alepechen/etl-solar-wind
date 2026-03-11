import pytest
import pandas as pd

from api_data_source.etl.loader import load
from api_data_source.etl.settings import OutputFormat
from api_data_source.etl.data_handler import DataHandler


SAMPLE_DF = pd.DataFrame([
    {"naive_timestamp": "2024-03-15", "variable": 1, "value": 99.5},
    {"naive_timestamp": "2024-03-16", "variable": 2, "value": 101.2},
])


class TestLoader:

    @pytest.mark.parametrize("output_format", list(OutputFormat))
    @pytest.mark.parametrize("dataset", ["windgen_generation", "solargen_generation"])
    def test_save_files_creates_file_with_correct_extension(
        self, output_format, dataset, tmp_path, monkeypatch
    ):
        monkeypatch.setattr("api_data_source.etl.loader.settings.OUTPUT_FOLDER", tmp_path)  

        path = load(SAMPLE_DF, dataset, fmt=output_format)

        assert path.exists()
        assert path.suffix == f".{output_format.value}"

    @pytest.mark.parametrize("output_format", list(OutputFormat))
    def test_save_files_content_is_not_empty(self, output_format, tmp_path, monkeypatch):
        monkeypatch.setattr("api_data_source.etl.loader.settings.OUTPUT_FOLDER", tmp_path) 

        path = load(SAMPLE_DF, "windgen_generation", fmt=output_format)

        assert path.stat().st_size > 0


class TestDataHandler:

    @pytest.fixture
    def raw_df(self):  # added self
        return pd.DataFrame([
            [1710000000000, 1, 99.5, 1710000000000],
            [1710086400000, 2, 101.2, 1710086400000],
        ])

    def test_clean_and_transform_returns_utc_timestamps(self, raw_df): 
        df = DataHandler().clean_and_transform(raw_df)
        assert str(df["naive_timestamp"].dtype) == "datetime64[ms, UTC]"

    def test_clean_and_transform_casts_numerics(self, raw_df):
        df = DataHandler().clean_and_transform(raw_df)
        assert pd.api.types.is_float_dtype(df["value"])