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
    """Return the elements for the top10 and top25 charts"""
    return soup.css.select('.songsList')


@task(cache_policy=NO_CACHE)
def parse_chart_data(soup: ResultSet[Tag], db_path: str) -> list[ChartEntry]:
    """Parse the part of the html page that contains information
    about charts - songs, the chart they are in and the place"""
    top10, top25 = soup

    chart_entries: list[ChartEntry] = []

    top10_songs = top10.select('.songLine')
    top25_songs = top25.select('.songLine')

    def parse_top_entries(
        type: ChartType, top: ResultSet[Tag], chart_element: Tag
    ) -> None:
        """Helper function to parse top10 and top25"""
        week_tag = chart_element.select_one('.songListDate')
        if week_tag is None:
            raise LookupError('No tag for date found in the parsed webpage')
        week: date = parse(week_tag.text, dayfirst=True).date()

        for song in top:
            with duckdb.connect(db_path) as conn:
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

            with duckdb.connect(db_path) as conn:
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

    parse_top_entries(type=ChartType.TOP10, top=top10_songs, chart_element=top10)
    parse_top_entries(type=ChartType.TOP25, top=top25_songs, chart_element=top25)

    return chart_entries


@task
def validate_charts_count(chart_entries: list[ChartEntry]) -> None:
    """Validate that scraped data has 10 ranked places for top10
    and 25 ranked places for top25."""
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
    """Insert the parsed chart data into polars dataframe"""
    web_chart = pl.DataFrame(chart_entries)
    web_chart = web_chart.drop('id')
    return web_chart


@task
def insert_chart_data_into_db(_new_chart_data: pl.DataFrame, db_path: str) -> None:
    """Write the dataframe of new chart data into database"""
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
    """Main flow to fetch the webpage, parse html, extract chart entries' elements
    and parse chart entries' data, load existing chart data to filter only new data
    and finaly insert chart data into database."""
    chart_soup = extract_chart_elements(soup=soup)
    charts = parse_chart_data(soup=chart_soup, db_path=db_path)
    validate_charts_count(chart_entries=charts)
    charts_df = create_charts_df(charts)
    insert_chart_data_into_db(charts_df, db_path)
