import logging.config
<<<<<<< HEAD
from fbdi.utils.config import config, DevConfig
import os
from watchtower import CloudWatchLogHandler
from fbdi.utils.s3_logger import S3LoggerHandler  # Import the S3LoggerHandler
from dotenv import load_dotenv

load_dotenv(override=True)

LOGS_DIR = 'logs'
os.makedirs(LOGS_DIR, exist_ok=True)

# S3 Logger Configuration
S3_BUCKET_NAME = "fbdi-logs"  
S3_LOG_FILE_PREFIX = "fbdi_logs"
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION_NAME = "us-east-1"
=======
from fbdi.utils.config import config , DevConfig
import os
from watchtower import CloudWatchLogHandler

LOGS_DIR = 'logs'
os.makedirs(LOGS_DIR , exist_ok=True)
>>>>>>> 10119d03bb4c74ec80f1aac0d08579e93c9cbebd

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
<<<<<<< HEAD
        "s3": {  # Add S3 handler
            "()": S3LoggerHandler,
            "bucket_name": S3_BUCKET_NAME,
            "log_file_prefix": S3_LOG_FILE_PREFIX,
            "aws_access_key": AWS_ACCESS_KEY,
            "aws_secret_key": AWS_SECRET_KEY,
            "region_name": AWS_REGION_NAME,
            "level": "DEBUG",
            "formatter": "detailed",
        },
=======
        # "cloudwatch": {  # Add CloudWatch handler
        #     "class": "watchtower.CloudWatchLogHandler",
        #     "formatter": "detailed",
        #     "level": "INFO",
        #     "log_group": "fbdi-logs",
        # },
>>>>>>> 10119d03bb4c74ec80f1aac0d08579e93c9cbebd
    },
    "loggers": {
        "fbdi": {
            "level": "DEBUG",
<<<<<<< HEAD
            "handlers": ["console", "file", "s3"],  # Include S3 handler
=======
            # "handlers": ["console", "file", "cloudwatch"],  # Include CloudWatch handler
            "handlers": ["console", "file"],  # Include CloudWatch handler
>>>>>>> 10119d03bb4c74ec80f1aac0d08579e93c9cbebd
            "propagate": False,
        },
    },
}

def configure_logger():
    logging.config.dictConfig(LOGGING_CONFIG)