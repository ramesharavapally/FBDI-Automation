# Most of this taken from Redowan Delowar's post on configurations with Pydantic
# https://rednafi.github.io/digressions/python/2020/06/03/python-configs.html
from functools import lru_cache
import os
from typing import Optional
from dotenv import load_dotenv
from pydantic_settings import BaseSettings, SettingsConfigDict
from watchtower import CloudWatchLogHandler

load_dotenv()

# print(os.getenv('ENV_STATE'))

class BaseConfig(BaseSettings):
    ENV_STATE: Optional[str] = os.getenv('ENV_STATE')

    """Loads the dotenv file. Including this is necessary to get
    pydantic to load a .env file."""
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")
    


class GlobalConfig(BaseConfig):
    DATABASE_URL: Optional[str] = None
    DB_FORCE_ROLL_BACK: bool = False
    SECRET_KEY: Optional[str] = None


class DevConfig(GlobalConfig):
    model_config = SettingsConfigDict(env_prefix="DEV_")


class ProdConfig(GlobalConfig):
    model_config = SettingsConfigDict(env_prefix="PROD_")


# class TestConfig(GlobalConfig):
#     DATABASE_URL: str = "sqlite:///test.db"
#     DB_FORCE_ROLL_BACK: bool = True

#     model_config = SettingsConfigDict(env_prefix="TEST_")


async def get_db_confg():
    env_state = os.getenv('ENV_STATE','DEV')

    if env_state=="DEV":
        db_username = os.getenv('DEV_DATABASE_USERNAME')
        db_passsord = os.getenv('DEV_DATABASE_PASSWORD')
        db_host = os.getenv('DEV_DATABASE_HOST')
        db_port = os.getenv('DEV_DATABASE_PORT')
        service_name = os.getenv('DEV_DATABASE_SERVICE_NAME')
    elif env_state=="PROD":
        db_username = os.getenv('PROD_DATABASE_USERNAME')
        db_passsord = os.getenv('PROD_DATABASE_PASSWORD')
        db_host = os.getenv('PROD_DATABASE_HOST')
        db_port = os.getenv('PROD_DATABASE_PORT')
        service_name = os.getenv('PROD_DATABASE_SERVICE_NAME')
    else:
        raise ValueError("Invalid ENV_STATE. Must be 'DEV' or 'PROD'.")
    
    return{
        "username": db_username,
        "password": db_passsord,
        "host": db_host,
        "port": db_port,
        "service_name": service_name,

    }


@lru_cache()
def get_config(env_state: str):
    """Instantiate config based on the environment."""
    # configs = {"DEV": DevConfig, "PROD": ProdConfig, "TEST": TestConfig}
    configs = {"DEV": DevConfig, "PROD": ProdConfig}
    return configs[env_state]()


config = get_config(BaseConfig().ENV_STATE)


# print(config)

