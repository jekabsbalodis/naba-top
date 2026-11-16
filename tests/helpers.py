"""Small builders for test data (HTML pages and a fake SQLMesh database)."""

import duckdb
from pydantic_settings import BaseSettings, PydanticBaseSettingsSource

from config_models import Config, StreamlitConfig


def song_line(name: str, place: int | str) -> str:
    """HTML for one song row. A place of 'j' means unranked, as on the real page."""
    return f"""
    <label class="songLine">
      <div class="leftLine">
        <div class="songPlace">{place}</div>
        <div class="songName">{name}</div>
      </div>
    </label>
    <div class="naba-top-song">
      <div class="song_vote_info"><div class="place_previous">{place}</div></div>
    </div>"""


def chart_html(lines: list[str], date: str | None = '13.02.2026') -> str:
    """HTML for one chart (a top10 or top25 block). `date=None` leaves out the date."""
    date_tag = f'<div class="newsCard__date songListDate">{date}</div>' if date else ''
    body = ''.join(lines)
    return f'<div class="songsList"><form>{body}{date_tag}</form></div>'


def page_html(top10: str, top25: str) -> str:
    return f'<html><body>{top10}{top25}</body></html>'


def naba_page(top10_ranked: int = 10, top25_ranked: int = 25, unranked: int = 5) -> str:
    """A whole page like the real one, with unique song names (45 songs by default)."""

    def chart(kind: str, ranked: int) -> str:
        lines = [
            song_line(f'Artist {kind} {i} - Song {kind} {i}', i)
            for i in range(1, ranked + 1)
        ]
        lines += [
            song_line(f'Artist {kind} New {i} - Song {kind} New {i}', 'j')
            for i in range(unranked)
        ]
        return chart_html(lines)

    return page_html(chart('Top10', top10_ranked), chart('Top25', top25_ranked))


def make_sqlmesh_db(path: str) -> None:
    """Create a small database shaped like the SQLMesh output.

    `mart.top10` is a normal model and should be uploaded. `sqlmesh__mart` is an
    internal SQLMesh schema and should be skipped.
    """
    with duckdb.connect(path) as conn:
        conn.execute('create schema mart')
        conn.execute(
            "create table mart.top10 as select 1 as place, 'Song' as song_name"
        )
        conn.execute('create schema sqlmesh__mart')
        conn.execute('create table sqlmesh__mart.mart__top10__1 as select 1 as x')


# `Config(...)` and `Config.model_validate(...)` read *three* sources: the dict you
# pass, environment variables and env.toml. These variants read only one, so a test
# checks one thing and never picks up values from the environment or a real env.toml.


class OnlyInitSource:
    """Read only the values passed in (the dict given to `model_validate`)."""

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        return (init_settings,)


class OnlyEnvSource:
    """Read only environment variables."""

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        return (env_settings,)


class ConfigFromDict(OnlyInitSource, Config):
    pass


class StreamlitConfigFromDict(OnlyInitSource, StreamlitConfig):
    pass


class ConfigFromEnv(OnlyEnvSource, Config):
    pass
