import logging
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from .settings import OutputFormat, Settings, settings

logger = logging.getLogger(__name__)


class LoadError(Exception):
    """Raised when persisting a DataFrame fails."""


def load(
    df: pd.DataFrame,
    dataset: str,
    fmt: OutputFormat = OutputFormat.CSV,
    cfg: Settings = settings,
) -> Path:
    """Persist a DataFrame to the output directory, partitioned by run date.

    The output path follows the Hive-style partitioning convention::

        <OUTPUT_FOLDER>/<dataset>/date=<YYYY-MM-DD>/data.<ext>

    Args:
        df: Transformed DataFrame to persist.
        dataset: Logical dataset name used as the top-level subdirectory,
            e.g. ``"windgen.csv_generation"``.
        fmt: Serialisation format. Defaults to :attr:`OutputFormat.CSV`.
        cfg: Application settings. Defaults to the module-level singleton.

    Returns:
        Absolute :class:`~pathlib.Path` of the written file.

    Raises:
        LoadError: If an unsupported format is supplied.
    """
    run_date = datetime.now(tz=timezone.utc).date()
    partition_dir = Path(cfg.OUTPUT_FOLDER) / dataset / f"date={run_date}"
    partition_dir.mkdir(parents=True, exist_ok=True)

    if fmt == OutputFormat.CSV:
        path = partition_dir / "data.csv"
        df.to_csv(path, index=False)

    elif fmt == OutputFormat.JSONL:
        path = partition_dir / "data.jsonl"
        df.to_json(path, orient="records", lines=True)

    else:
        raise LoadError(
            f"Unsupported output format '{fmt}'. "
            f"Valid options: {[f.value for f in OutputFormat]}"
        )

    logger.info("Wrote %d rows to %s", len(df), path)
    return path