import os
import sys
import tomllib
from pathlib import Path
from typing import Annotated

from pydantic import AfterValidator, BaseModel, EmailStr, HttpUrl

basedir = Path(__file__).resolve().parent

IS_TESTING = os.getenv('TESTING', 'false').lower() == 'true'

config_file = 'config.test.toml' if IS_TESTING else 'config.toml'

try:
    with open(basedir / config_file, 'rb') as f:
        _config = tomllib.load(f)
except FileNotFoundError as e:
    print('Check if file exists.', file=sys.stderr)
    print(e, file=sys.stderr)
    sys.exit()


def validate_queries_path(v: Path) -> Path:
    p = (basedir / v).resolve()
    if not p.exists():
        raise FileNotFoundError(f'Queries path does not exist - {p}')
    return p


def validate_db_path(v: Path | str) -> Path | str:
    if v == ':memory:':
        return v
    p = (basedir / v).resolve()
    if not p.parent.exists():
        raise FileNotFoundError(f'DB path does not exist - {p}')
    return p


class Config(BaseModel):
    DB_PATH: Annotated[Path | str, AfterValidator(validate_db_path)]
    QUERIES_PATH: Annotated[Path, AfterValidator(validate_queries_path)]
    FLOW_URL: HttpUrl
    FLOW_EMAIL: EmailStr


config = Config(
    DB_PATH=_config['db']['path'],
    QUERIES_PATH=_config['db']['queries'],
    FLOW_URL=_config['flows']['url'],
    FLOW_EMAIL=_config['flows']['email'],
)
