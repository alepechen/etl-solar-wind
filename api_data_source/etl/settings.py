from enum import Enum
from pathlib import Path
import os

from pydantic_settings import BaseSettings
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("API_KEY")
class OutputFormat(str, Enum):
    """Supported output serialisation formats."""

    CSV = "csv"
    JSONL = "jsonl"


class Settings(BaseSettings):

    # -- Data source ----------------------------------------------------------
    API_KEY: str=API_KEY
    DATA_SOURCE_BASE_URL: str = "http://localhost:8000"

    # -- HTTP client ----------------------------------------------------------
    HTTP_TIMEOUT: int = 10
    HTTP_MAX_RETRIES: int = 3
    HTTP_RETRY_MIN_WAIT: int = 1
    HTTP_RETRY_MAX_WAIT: int = 10

    # -- Output ---------------------------------------------------------------
    OUTPUT_FOLDER: Path = Path(__file__).resolve().parent.parent / "output"
    OUTPUT_FORMAT: OutputFormat = OutputFormat.CSV



settings = Settings()