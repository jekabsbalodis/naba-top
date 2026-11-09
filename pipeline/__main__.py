"""Command line entry point - defines one command per pipeline step."""

import os
from datetime import datetime
from pathlib import Path

import polars as pl
import typer
from bs4 import BeautifulSoup

import config as global_config
from pipeline import config as pipeline_config
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
    url, email = pipeline_config.scrape_target()
    response = fetch_webpage(url, email)
    (pipeline_config.stage_dir() / PAGE).write_text(response.text, encoding='utf-8')


@app.command()
def parse_songs() -> None:
    """Parse songs from the saved HTML file into songs.parquet."""
    stage = pipeline_config.stage_dir()
    songs = parse_song_data(extract_song_elements(_soup(stage)))
    create_songs_df(songs).write_parquet(stage / SONGS)


@app.command()
def load_songs() -> None:
    """Insert songs from parquet file into the database."""
    insert_songs_into_db(
        pl.read_parquet(pipeline_config.stage_dir() / SONGS), pipeline_config.db_path()
    )


@app.command()
def parse_charts() -> None:
    """Parse charts from the saved HTML file into charts.parquet."""
    stage = pipeline_config.stage_dir()
    entries = parse_chart_data(
        extract_chart_elements(_soup(stage)), pipeline_config.db_path()
    )
    create_charts_df(entries).write_parquet(stage / CHARTS)


@app.command()
def validate_charts() -> None:
    """Fail if charts does not have the expected number of ranked rows."""
    validate_charts_count(pl.read_parquet(pipeline_config.stage_dir() / CHARTS))


@app.command()
def load_charts() -> None:
    """Insert charts from parquet file into the database."""
    insert_chart_data_into_db(
        pl.read_parquet(pipeline_config.stage_dir() / CHARTS), pipeline_config.db_path()
    )


@app.command()
def upload() -> None:
    """Upload the database tables and views to S3 storage as parquet."""
    if global_config.get_environment() == global_config.Environment.PROD:
        upload_data(
            sqlm_d=pipeline_config.sqlm_data_db_path(),
            s3_config=pipeline_config.s3_config(),
        )
    elif global_config.get_environment() == global_config.Environment.DEV:
        upload_data(
            sqlm_d=pipeline_config.sqlm_data_db_path(),
            local_dir=global_config.local_upload_dir(),
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
        upload,
    ):
        typer.echo(f'==> {step.__name__}', err=True)
        step()


if __name__ == '__main__':
    app()
