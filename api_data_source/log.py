import logging
import logging.config

from structlog import configure
from structlog.processors import JSONRenderer, TimeStamper, add_log_level
from structlog.stdlib import LoggerFactory, ProcessorFormatter

LOG_LEVEL = "INFO"
LOG_FORMAT = "json"


def configure_logging() -> None:
    configure(
        processors=[
            add_log_level,
            TimeStamper(fmt="iso"),
            ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=LoggerFactory(),
    )

    logging_config = {
        "version": 1,
        "disable_existing_loggers": True,
        "formatters": {
            "json": {
                "()": ProcessorFormatter,
                "processor": JSONRenderer(),
            },
        },
        "handlers": {
            "json": {
                "level": LOG_LEVEL,
                "class": "logging.StreamHandler",
                "formatter": "json",
            },
        },
        "loggers": {
            "root": {"handlers": [LOG_FORMAT], "level": LOG_LEVEL},
            "uvicorn.error": {
                "handlers": [LOG_FORMAT], "level": LOG_LEVEL, "propagate": False
            },
            "uvicorn.access": {
                "handlers": [LOG_FORMAT], "level": LOG_LEVEL, "propagate": False
            },
        },
    }
    logging.config.dictConfig(logging_config)
