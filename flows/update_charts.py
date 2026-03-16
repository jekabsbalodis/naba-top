"""Chart data processing workflow for naba-top application.

Contains Prefect tasks and flows for extracting, parsing, validating,
and storing music chart data from web pages.
"""

from datetime import date

import duckdb
import polars as pl
from bs4 import BeautifulSoup, ResultSet, Tag
from dateutil.parser import parse
from prefect import flow, task
from prefect.cache_policies import NO_CACHE

from models import ChartEntry, ChartType


@task
def extract_chart_elements(soup: BeautifulSoup) -> ResultSet[Tag]:
    """Extract chart container elements from parsed HTML.

    Args:
        soup: BeautifulSoup parsed document.

    Returns:
        ResultSet containing top10 and top25 chart elements.

    Uses CSS selector '.songsList' to find chart containers.

    """
    return soup.css.select('.songsList')


@task(cache_policy=NO_CACHE)
def parse_chart_data(soup: ResultSet[Tag], db_path: str) -> list[ChartEntry]:
    """Parse chart data from HTML elements into structured entries.

    Extracts song positions, chart types, dates, and new entry status
    from webpage HTML and maps to existing song IDs in database.

    Args:
        soup: ResultSet containing chart HTML elements.
        db_path: Path to DuckDB database for song lookup.

    Returns:
        List of ChartEntry objects with song_id, chart_type, place, week, is_new_entry

    Raises:
        LookupError: If required HTML elements are missing or songs not found in DB.

    Processes both top10 and top25 charts, determining new entries by
    comparing against existing chart data.

    """
    top10, top25 = soup

    chart_entries: list[ChartEntry] = []

    top10_songs = top10.select('.songLine')
    top25_songs = top25.select('.songLine')

    def _parse_top_entries(
        type: ChartType, top: ResultSet[Tag], chart_element: Tag
    ) -> None:
        """Parse chart entries from HTML elements for a specific chart type.

        Extracts song positions, dates, and new entry status from webpage HTML
        and maps to existing song IDs in database.

        Args:
            type: ChartType (TOP10 or TOP25) being processed.
            top: ResultSet containing song HTML elements for this chart.
            chart_element: Tag containing the chart date information.

        Raises:
            LookupError: If required HTML elements are missing or songs not found in DB.

        Determines new entries by comparing against existing chart data in database.
        Adds parsed entries to the chart_entries list.

        """
        week_tag = chart_element.select_one('.songListDate')
        if week_tag is None:
            raise LookupError('No tag for date found in the parsed webpage')
        week: date = parse(week_tag.text, dayfirst=True).date()

        for song in top:
            with duckdb.connect(db_path, read_only=True) as conn:
                web_songname_tag = song.select_one('.songName')
                if web_songname_tag is None:
                    raise LookupError(
                        'No tag for web_songname found in the parsed webpage'
                    )
                web_songname: str = web_songname_tag.text
                song_id_res = conn.sql(
                    """-- sql
                    select
                        id
                    from
                        songs
                    where
                        web_songname = ?;
                    """,
                    params=[web_songname],
                ).fetchone()
            if song_id_res is None:
                raise LookupError(
                    f"""Song {web_songname} not found in database,
                    check if song update flow has ran."""
                )
            song_id = song_id_res[0]

            place_tag = song.select_one('.songPlace')
            if place_tag is None:
                raise LookupError(
                    f'Song {web_songname} has no place tag, check webpage for errors.'
                )
            try:
                place: int | None = int(place_tag.text)
            except ValueError:
                place = None

            with duckdb.connect(db_path, read_only=True) as conn:
                old_entry_res = conn.sql(
                    """-- sql
                    select
                        1
                    from
                        charts
                    where
                        song_id = ?;
                    """,
                    params=[song_id],
                ).fetchone()

            if old_entry_res is not None:
                old_entry = True
            else:
                old_entry = False

            if place is None or old_entry is False:
                is_new_entry = True
            else:
                is_new_entry = False

            entry = ChartEntry(
                song_id=song_id,
                chart_type=type,
                place=place,
                week=week,
                is_new_entry=is_new_entry,
            )

            chart_entries.append(entry)

    _parse_top_entries(type=ChartType.TOP10, top=top10_songs, chart_element=top10)
    _parse_top_entries(type=ChartType.TOP25, top=top25_songs, chart_element=top25)

    return chart_entries


@task
def validate_charts_count(chart_entries: list[ChartEntry]) -> None:
    """Validate that parsed charts contain expected number of ranked entries.

    Ensures top10 has exactly 10 ranked songs and top25 has exactly 25.

    Args:
        chart_entries: List of parsed chart entries.

    Raises:
        ValueError: If actual ranked entry count doesn't match expected count.

    Only counts entries with non-None place values.

    """
    expected_count = {ChartType.TOP10: 10, ChartType.TOP25: 25}

    actual_count = dict.fromkeys(ChartType, 0)

    for entry in chart_entries:
        if entry.place is None:
            continue
        actual_count[entry.chart_type] += 1

    for chart_type, expected in expected_count.items():
        actual = actual_count[chart_type]
        if actual != expected:
            raise ValueError(
                f"""{chart_type} has {actual} ranked entries, {expected} were expected.
                Check webpage for errors."""
            )


@task
def create_charts_df(chart_entries: list[ChartEntry]) -> pl.DataFrame:
    """Convert chart entries to Polars DataFrame for database insertion.

    Args:
        chart_entries: List of ChartEntry objects.

    Returns:
        Polars DataFrame ready for database insertion.

    Drops 'id' column as it's auto-generated by database.

    """
    web_chart = pl.DataFrame(chart_entries)
    web_chart = web_chart.drop('id')
    return web_chart


@task
def insert_chart_data_into_db(_new_chart_data: pl.DataFrame, db_path: str) -> None:
    """Insert chart data DataFrame into database using INSERT OR IGNORE.

    Args:
        _new_chart_data: DataFrame containing chart data to insert.
        db_path: Path to DuckDB database.

    Uses DuckDB's INSERT OR IGNORE to avoid duplicate entries.

    """
    with duckdb.connect(db_path) as conn:
        conn.execute(
            """-- sql
            insert or ignore into charts (
                song_id, chart_type, place, week, is_new_entry
            ) (
                select * from _new_chart_data
            )
            """
        )


@flow
def update_charts_flow(soup: BeautifulSoup, db_path: str) -> None:
    """Complete workflow for updating chart data in database.

    Orchestrates the full process of extracting, parsing, validating,
    and storing chart information from web pages.

    Args:
        soup: BeautifulSoup parsed webpage.
        db_path: Path to DuckDB database.

    """
    chart_soup = extract_chart_elements(soup=soup)
    charts = parse_chart_data(soup=chart_soup, db_path=db_path)
    validate_charts_count(chart_entries=charts)
    charts_df = create_charts_df(charts)
    insert_chart_data_into_db(charts_df, db_path)
