from datetime import date
from typing import Any, Dict, Union
from fastapi import Query
import uvicorn
from fastapi import FastAPI, responses
from fastapi.openapi.utils import get_openapi

from api_data_source.backend import generate_csv_response, generate_json_response
from api_data_source.log import configure_logging
from api_data_source.middleware import BlockHosts
from api_data_source.etl.etl_runner import run_pipeline

app = FastAPI()

def custom_openapi() -> Dict[str, Any]:
    if app.openapi_schema:
        return app.openapi_schema
    
    openapi_schema = get_openapi(
        title="API Data Source",
        version="2.5.11",
        description="""## Description
This API Data Source provides a range of weather model data can be provided in a CSV or JSON format.


## Request Format
- Request Format: `http://127.0.0.1:8000/[endpoint]?api_key=[api_key]`

- Where `[endpoint]` is a path from the endpoint documentation below.

- The `requested_date` format: `YYYY-MM-DD`

- Valid `api_key:ADU8S67Ddy!d7f?`


## Authorization

- The weather model endpoints require valid `api_key` to access.

- Unauthorized requests will return a http status code of 403 - "Forbidden"


## API Rate Limits

- Due to demand, requests can be throttled at any time.

- When the throttle limit is hit, you will receive a http status code of 429 - "Too many requests"

        """,
        routes=app.routes,
    )
    app.openapi_schema = openapi_schema
    return app.openapi_schema


app.openapi = custom_openapi
app.add_middleware(BlockHosts)


@app.get("/status", tags=["Health Check"])
def status():
    return {"status": "ok"}


@app.get("/{requested_date}/renewables/windgen.csv", tags=["Wind"])
async def wind_generation(
    api_key: Union[str, None],
    requested_date: Union[date, None],
) -> responses.StreamingResponse:
    return generate_csv_response(requested_date)


@app.get("/{requested_date}/renewables/solargen.json", tags=["Solar"])
async def solar_generation(
    api_key: Union[str, None],
    requested_date: Union[date, None],
) -> responses.Response:
    return generate_json_response(requested_date)

@app.get("/{requested_date}/output")
async def data_generation(
    requested_date: str,
    api_key: str = Query(...),  # query param
):
    return await run_pipeline(requested_date)

if __name__ == "__main__":
    configure_logging()
    uvicorn.run(app, log_config=None, port=8000)
