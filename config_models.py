"""Models for the app settings."""

from pathlib import Path, PurePosixPath
from typing import Annotated, Literal, Self

from pydantic import (
    AfterValidator,
    BaseModel,
    ConfigDict,
    EmailStr,
    HttpUrl,
    SecretStr,
    StringConstraints,
    model_validator,
)
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
    TomlConfigSettingsSource,
)

ENV_PREFIX: str = 'NABA_TOP_'
ENV_FILE: Path = Path(__file__).parent / 'env.toml'


def _path_is_absolute(p: Path) -> Path:
    if not p.is_absolute():
        raise ValueError('Provide an absolute path in the config file.')
    return p


def _secret_is_not_empty(s: SecretStr) -> SecretStr:
    if not s.get_secret_value().strip():
        raise ValueError('Provide a secret value that is not empty.')
    return s


def _relative_path_inside_data_dir(value: str) -> str:
    path = PurePosixPath(value)
    if (
        '\\' in value
        or value.endswith('/')
        or path.is_absolute()
        or '..' in path.parts
        or not path.name
    ):
        raise ValueError(
            'Use a relative path with `/` inside `data_dir`, '
            f'like `database/raw.duckdb`.\n{value}\n{path}'
        )
    return value


NonEmptyStr = Annotated[str, StringConstraints(min_length=1, strip_whitespace=True)]
NonEmptySecret = Annotated[SecretStr, AfterValidator(_secret_is_not_empty)]
DbFileName = Annotated[
    str,
    StringConstraints(min_length=1, strip_whitespace=True),
    AfterValidator(_relative_path_inside_data_dir),
]
AbsolutePath = Annotated[Path, AfterValidator(_path_is_absolute)]


class Strict(BaseModel):
    """Set the model to forbid extra keys in the dict."""

    model_config = ConfigDict(extra='forbid', frozen=True)


class S3Keys(Strict):
    """Config for the S3 bucket access keys."""

    key_id: NonEmptyStr
    secret: NonEmptySecret


class Garage(Strict):
    """Config for the garage endpoint and region."""

    endpoint: NonEmptyStr
    region: NonEmptyStr


class StreamlitGarage(Garage):
    """Config for the Streamlit garage access, only read key."""

    model_config = ConfigDict(extra='ignore')

    read: S3Keys


class FullGarage(StreamlitGarage):
    """Full config for the garage access, both read and write keys."""

    model_config = ConfigDict(extra='forbid')

    write: S3Keys


class Auth(Strict):
    """Config for the streamlit auth module."""

    redirect_uri: HttpUrl
    cookie_secret: NonEmptySecret
    client_id: NonEmptyStr
    client_secret: NonEmptySecret
    server_metadata_url: HttpUrl


class StreamlitData(Strict):
    """Data config for streamlit with only the local files."""

    model_config = ConfigDict(extra='ignore')

    data_dir: AbsolutePath

    @property
    def upload_dir(self) -> Path:
        """Folder with local parquet files."""
        return self.data_dir / 'upload'

    def require_data_dir(self) -> Path:
        """Check if `data_dir` exists and fail if it doesn't."""
        if not self.data_dir.is_dir():
            raise FileNotFoundError(
                f'`data_dir` does not exist (`{self.data_dir}`).'
                'Create it or fix `env.toml`.'
            )
        return self.data_dir

    def require_upload_dir(self) -> Path:
        """Require upload dir and fail if it doesn't or doesn't have files int it."""
        if not self.upload_dir.is_dir():
            raise FileNotFoundError(
                f'Directory with parquet files does not exist (`{self.upload_dir}`).'
                ' Create it or fix `env.toml`.'
            )
        if not (self.upload_dir / 'manifest.json').is_file():
            raise FileNotFoundError(
                f'`manifest.json` is missing from {self.upload_dir}.'
            )
        if not any(self.upload_dir.rglob('*.parquet')):
            raise FileNotFoundError(f'No parquet files found in the {self.upload_dir}.')
        return self.upload_dir


class Data(StreamlitData):
    """Config for the data folder and databases."""

    model_config = ConfigDict(extra='forbid')

    raw_data_db: DbFileName
    sqlmesh_data_db: DbFileName
    sqlmesh_state_db: DbFileName

    @property
    def raw_data_path(self) -> Path:
        """Full path of the raw scraped data database."""
        return self.data_dir / self.raw_data_db

    @property
    def sqlmesh_data_path(self) -> Path:
        """Full path of the database holding SQLMesh models."""
        return self.data_dir / self.sqlmesh_data_db

    @property
    def sqlmesh_state_path(self) -> Path:
        """Full path of the SQLMesh state database."""
        return self.data_dir / self.sqlmesh_state_db

    @model_validator(mode='after')
    def _check_databases_are_different_files(self) -> Self:
        paths = {
            'raw_dta_db': self.raw_data_path,
            'sqlmesh_data_db': self.sqlmesh_data_path,
            'sqlmesh_state_db': self.sqlmesh_state_path,
        }
        if len(set(paths.values())) != len(paths):
            raise ValueError(f"Each database needs it's own file, got: {paths}")
        return self

    def ensure_upload_dir(self) -> Path:
        """Create upload dir if it does not exist."""
        self.require_data_dir()
        self.upload_dir.mkdir(exist_ok=True)
        return self.upload_dir

    def ensure_database_dirs(self) -> None:
        """Create the folder of the database files."""
        self.require_data_dir()
        for path in (
            self.raw_data_path,
            self.sqlmesh_data_path,
            self.sqlmesh_state_path,
        ):
            path.parent.mkdir(parents=True, exist_ok=True)

    def stage_dir(self, run_id: str) -> Path:
        """Create and return stage dir with run_id subfolder inside data dir."""
        self.require_data_dir()
        path = self.data_dir / 'stage' / run_id
        path.mkdir(exist_ok=True, parents=True)
        return path


class Scrape(Strict):
    """config for the scrape target and header email."""

    url: HttpUrl
    email: EmailStr


class Pipeline(Strict):
    """Config for the dagu pipeline description."""

    ntfy_url: HttpUrl


class StreamlitConfig(BaseSettings):
    """Config for the streamlit app."""

    model_config = SettingsConfigDict(
        env_prefix=ENV_PREFIX,
        env_nested_delimiter='__',
        toml_file=ENV_FILE,
        extra='ignore',
        frozen=True,
    )

    environment: Literal['production', 'development']
    garage: StreamlitGarage
    auth: Auth
    data: StreamlitData | None = None

    @model_validator(mode='after')
    def _check_auth_urls(self) -> Self:
        if self.auth.server_metadata_url.scheme != 'https':
            raise ValueError('Server metadata url should be `https`.')
        if (
            self.environment == 'production'
            and self.auth.redirect_uri.scheme != 'https'
        ):
            raise ValueError('Redirect uri should be `https` in production.')
        return self

    @model_validator(mode='after')
    def _check_data_in_development(self) -> Self:
        if self.environment == 'development' and self.data is None:
            raise ValueError('[Data] table with `data_dir` is required in development.')
        return self

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        """Define custom sources and order for the pydantic-settings BaseSettings."""
        return (
            init_settings,
            env_settings,
            TomlConfigSettingsSource(settings_cls),
            dotenv_settings,
            file_secret_settings,
        )


class Config(StreamlitConfig):
    """Config for the whole app.

    Includes values for the pipeline, SQLMesh.
    """

    model_config = SettingsConfigDict(extra='forbid')

    garage: FullGarage
    data: Data
    scrape: Scrape
    pipeline: Pipeline

    @model_validator(mode='after')
    def _check_pipeline_urls(self) -> Self:
        urls = {
            'scrape.url': self.scrape.url,
            'pipeline.ntfy_url': self.pipeline.ntfy_url,
        }
        for name, url in urls.items():
            if url.scheme != 'https':
                raise ValueError(f'{name} should be `https`.')
        return self
