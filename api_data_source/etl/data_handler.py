import logging
from typing import Union

import pandas as pd

logger = logging.getLogger(__name__)

RAW_COLUMNS = ["naive_timestamp", "variable", "value", "last_modified_utc"]
NUMERIC_COLUMNS = ("variable", "value")
TIMESTAMP_COLUMNS = ("naive_timestamp", "last_modified_utc")


def _parse_timestamp(value: Union[int, str]) -> pd.Timestamp:
    if isinstance(value, (int, float)):
        return pd.to_datetime(value, unit="ms", utc=True)
    return pd.to_datetime(value, utc=True)


class DataHandler:
    
    def clean_and_transform(self, df: pd.DataFrame) -> pd.DataFrame:
        if len(df.columns) != len(RAW_COLUMNS):
            raise ValueError(
                f"Expected {len(RAW_COLUMNS)} columns, got {len(df.columns)}. "
                f"Check the API schema."
            )

        df = df.copy()
        df.columns = RAW_COLUMNS

        for col in TIMESTAMP_COLUMNS:
            df[col] = df[col].apply(_parse_timestamp)
            logger.debug("Parsed timestamp column '%s'.", col)

        for col in NUMERIC_COLUMNS:
            if col in df.columns:
                before_nulls = df[col].isna().sum()
                df[col] = pd.to_numeric(df[col], errors="coerce")
                new_nulls = df[col].isna().sum() - before_nulls
                if new_nulls:
                    logger.warning(
                        "Coerced %d non-numeric value(s) to NaN in column '%s'.",
                        new_nulls,
                        col,
                    )

        df.columns = (
            df.columns.str.strip().str.lower().str.replace(" ", "_", regex=False)
        )

        return df