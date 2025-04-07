import logging.config
from fbdi.utils.config import config , DevConfig
import os
from watchtower import CloudWatchLogHandler

LOGS_DIR = 'logs'
os.makedirs(LOGS_DIR , exist_ok=True)

LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "filters": {
        "correlation_id": {
            '()': "asgi_correlation_id.CorrelationIdFilter",
            "uuid_length": 8 if isinstance(config, DevConfig) else 32,
            "default_value": "-",
        },
    },
    "formatters": {
        "default": {
            "format": "%(asctime)s - %(name)s - %(levelname)s - [%(correlation_id)s] - %(message)s"
        },
        "detailed": {
            "format": "%(asctime)s - %(name)s - %(levelname)s - [%(correlation_id)s] - [%(filename)s:%(lineno)d:%(funcName)s] - %(message)s"
        },
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "default",
            'filters': ['correlation_id'],
            "level": "DEBUG",
        },
        "file": {
            "class": "logging.handlers.RotatingFileHandler",
            "formatter": "detailed",
            "level": "DEBUG",
            'filters': ['correlation_id'],
            "filename": "logs/fbdi.log",
            "maxBytes": 5 * 1024 * 1024,  # 5 MB
            "backupCount": 5,
            "encoding": "utf8",
        },
        # "cloudwatch": {  # Add CloudWatch handler
        #     "class": "watchtower.CloudWatchLogHandler",
        #     "formatter": "detailed",
        #     "level": "INFO",
        #     "log_group": "fbdi-logs",
        # },
    },
    "loggers": {
        "fbdi": {
            "level": "DEBUG",
            # "handlers": ["console", "file", "cloudwatch"],  # Include CloudWatch handler
            "handlers": ["console", "file"],  # Include CloudWatch handler
            "propagate": False,
        },
    },
}

def configure_logger():
    logging.config.dictConfig(LOGGING_CONFIG)