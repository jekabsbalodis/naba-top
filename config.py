import os
import sys
import tomllib
from pathlib import Path
from typing import Annotated

from pydantic import AfterValidator, BaseModel, EmailStr, HttpUrl

basedir = Path(__file__).resolve().parent

NABA_TOP_ENV = os.getenv('NABA_TOP_ENV', 'prod').lower()

config_map: dict[str, str] = {
    'prod': 'config.toml',
    'test': 'config.test.toml',
    'dev': 'config.dev.toml',
}

try:
    config_file = config_map[NABA_TOP_ENV]
except KeyError as ke:
    raise ValueError(
        f'Invalid ENV {NABA_TOP_ENV}. Must be one of {list(config_map.keys())}'
    ) from ke

try:
    with open(basedir / config_file, 'rb') as f:
        _config = tomllib.load(f)
except FileNotFoundError as e:
    print('Check if file exists.', file=sys.stderr)
    print(e, file=sys.stderr)
    sys.exit()


def validate_db_path(v: Path | str) -> Path | str:
    p = (basedir / v).resolve()
    if not p.parent.exists():
        raise FileNotFoundError(f'DB path does not exist - {p}')
    return p


def validate_httpurl(v: str) -> str:
    HttpUrl(v)
    return v


class Config(BaseModel):
    DB_PATH: Annotated[Path, AfterValidator(validate_db_path)]
    FLOW_URL: Annotated[str, AfterValidator(validate_httpurl)]
    FLOW_EMAIL: EmailStr
    CLIENT_ID: str
    SERVER_METADATA_URL: Annotated[str, AfterValidator(validate_httpurl)]
    EXPECTED_ISSUER: Annotated[str, AfterValidator(validate_httpurl)]
    CALLBACK_URL: Annotated[str, AfterValidator(validate_httpurl)]
    SESSION_SECRET: str
    STORAGE_SECRET: str
    HOST: str
    PORT: int


config = Config(
    DB_PATH=_config['db']['path'],
    FLOW_URL=_config['flows']['url'],
    FLOW_EMAIL=_config['flows']['email'],
    CLIENT_ID=_config['auth']['client_id'],
    SERVER_METADATA_URL=_config['auth']['server_metadata_url'],
    EXPECTED_ISSUER=_config['auth']['expected_issuer'],
    CALLBACK_URL=_config['auth']['callback_url'],
    SESSION_SECRET=_config['auth']['session_secret'],
    STORAGE_SECRET=_config['auth']['storage_secret'],
    HOST=_config['auth']['host'],
    PORT=_config['auth']['port'],
)
