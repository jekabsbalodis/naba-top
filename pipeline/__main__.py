"""Command line entry point - defines one command per pipeline step."""

import os
from datetime import datetime
from pathlib import Path

import polars as pl
import typer
from bs4 import BeautifulSoup
from sqlmesh import Context

from models import S3Config
from pipeline.config import config, stage_dir
from pipeline.shared_tasks import fetch_webpage, upload_data
from pipeline.update_charts import (
    create_charts_df,
    extract_chart_elements,
    insert_chart_data_into_db,
    parse_chart_data,
    validate_charts_count,
)
from pipeline.update_songs import (
    create_songs_df,
    extract_song_elements,
    insert_songs_into_db,
    parse_song_data,
)

app = typer.Typer(add_completion=False, no_args_is_help=True)

PAGE = 'page.html'
SONGS = 'songs.parquet'
CHARTS = 'charts.parquet'


def _soup(stage: Path) -> BeautifulSoup:
    return BeautifulSoup((stage / PAGE).read_text(encoding='utf-8'), 'lxml')


@app.command()
def fetch() -> None:
    """Download the page and save the raw HTML."""
    url = config.scrape.url.encoded_string()
    email = config.scrape.email
    response = fetch_webpage(url, email)
    (stage_dir() / PAGE).write_text(response.text, encoding='utf-8')


@app.command()
def parse_songs() -> None:
    """Parse songs from the saved HTML file into songs.parquet."""
    stage = stage_dir()
    songs = parse_song_data(extract_song_elements(_soup(stage)))
    create_songs_df(songs).write_parquet(stage / SONGS)


@app.command()
def load_songs() -> None:
    """Insert songs from parquet file into the database."""
    insert_songs_into_db(pl.read_parquet(stage_dir() / SONGS), config.data.raw_data_db)


@app.command()
def parse_charts() -> None:
    """Parse charts from the saved HTML file into charts.parquet."""
    stage = stage_dir()
    entries = parse_chart_data(
        extract_chart_elements(_soup(stage)), config.data.raw_data_db
    )
    create_charts_df(entries).write_parquet(stage / CHARTS)


@app.command()
def validate_charts() -> None:
    """Fail if charts does not have the expected number of ranked rows."""
    validate_charts_count(pl.read_parquet(stage_dir() / CHARTS))


@app.command()
def load_charts() -> None:
    """Insert charts from parquet file into the database."""
    insert_chart_data_into_db(
        pl.read_parquet(stage_dir() / CHARTS), config.data.raw_data_db
    )


@app.command()
def update_models() -> None:
    """Run SQLMesh transformation (same as `sqlmesh -p data_transform run`)."""
    os.environ['NABA_TOP_DATA__DATA_DIR'] = str(config.data.data_dir)
    os.environ['NABA_TOP_DATA__RAW_DATA_DB'] = config.data.raw_data_db
    os.environ['NABA_TOP_DATA__SQLMESH_DATA_DB'] = config.data.sqlmesh_data_db
    os.environ['NABA_TOP_DATA__SQLMESH_STATE_DB'] = config.data.sqlmesh_state_db

    status = Context(paths='data_transform').run()
    if status.is_failure:
        raise RuntimeError(f'SQLMesh run failed: {status}')


@app.command()
def upload() -> None:
    """Upload the database tables and views to S3 storage as parquet."""
    if config.environment == 'production':
        upload_data(
            sqlm_d=config.data.sqlmesh_data_db,
            s3_config=S3Config(
                key_id=config.garage.write.key_id,
                secret=config.garage.write.secret.get_secret_value(),
                endpoint=config.garage.endpoint,
                region=config.garage.region,
            ),
        )
    else:
        upload_data(
            sqlm_d=config.data.sqlmesh_data_db,
            local_dir=config.data.ensure_upload_dir(),
        )


@app.command()
def run_all() -> None:
    """Run all the steps in the correct order, but as one process.

    If executed without Dagu, there is no run id.
    Id is generated unless NABA_TOP_RUN_ID is already set.
    """
    os.environ.setdefault('NABA_TOP_RUN_ID', datetime.now().strftime('%Y%m%d_%H%M%S'))

    for step in (
        fetch,
        parse_songs,
        load_songs,
        parse_charts,
        validate_charts,
        load_charts,
        update_models,
        upload,
    ):
        typer.echo(f'==> {step.__name__}', err=True)
        step()


if __name__ == '__main__':
    app()
