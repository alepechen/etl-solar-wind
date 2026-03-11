import io

import numpy as np
import pandas as pd
from fastapi import responses

RND = np.random.default_rng(np.random.PCG64(seed=0))


def generate_dataframe(requested_date: str) -> pd.DataFrame:
    start_date = pd.Timestamp(requested_date, tz="UTC")
    end_date = start_date + pd.Timedelta(days=1)
    aggregation = "5min"

    naive_timestamps = pd.date_range(
        start=start_date,
        end=end_date,
        freq=aggregation,
    )

    return pd.DataFrame(
        data={
            "Naive_Timestamp ": naive_timestamps,
            " Variable": RND.integers(0, 1000, naive_timestamps.size),
            "value": RND.random(naive_timestamps.size) * 100 - 50,
            "Last Modified utc": pd.Timestamp(requested_date, tz="UTC"),
        }
    )


def generate_csv_response(requested_date: str) -> responses.StreamingResponse:
    df = generate_dataframe(requested_date)
    stream = io.StringIO()
    df.to_csv(stream, index=False)
    response = responses.StreamingResponse(
        content=iter([stream.getvalue()]),
        media_type="text/csv",
    )
    response.headers["Content-Disposition"] = f"attachment; filename={requested_date}.csv"
    response.headers["Content-Type"] = "text/csv"
    return response


def generate_json_response(requested_date: str) -> responses.Response:
    df = generate_dataframe(requested_date)
    return responses.Response(
        df.to_json(orient="records"), media_type="application/json"
    )
