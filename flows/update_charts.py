from datetime import date

import duckdb
import polars as pl
from bs4 import BeautifulSoup, ResultSet, Tag
from dateutil.parser import parse
from prefect import flow, task

from config import config
from models import ChartEntry, ChartType


@task
def extract_chart_elements(soup: BeautifulSoup) -> ResultSet[Tag]:
    """Return the elements for the top10 and top25 charts"""
    return soup.css.select('.songsList')


@task
def parse_chart_data(soup: ResultSet[Tag]) -> list[ChartEntry]:
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
        if week_tag is not None:
            week: date = parse(week_tag.text, dayfirst=True).date()

        for song in top:
            info = song.find_next_sibling('div', class_='naba-top-song')

            with duckdb.connect(config.DB_PATH) as conn:
                web_songname_tag = song.select_one('.songName')
                if web_songname_tag is not None:
                    web_songname: str = web_songname_tag.text
                song_id_res = conn.sql(
                    'select id from songs where web_songname = ?',
                    params=[web_songname],
                ).fetchone()
            if song_id_res is not None:
                song_id = song_id_res[0]

            place_tag = song.select_one('.songPlace')
            if place_tag is not None:
                try:
                    place: int | None = int(place_tag.text)
                except ValueError:
                    place = None

            is_new_entry_tag = info.select_one('.place_previous')
            if is_new_entry_tag is not None:
                is_new_entry = True if 'j' in str(is_new_entry_tag.text) else False

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
def load_existing_chart_data() -> pl.DataFrame:
    """Query database for existing charts"""
    with duckdb.connect(config.DB_PATH) as conn:
        df = conn.sql('select * from charts').pl()

    df.drop('id')
    return df


@task
def create_web_charts_df(chart_entries: list[ChartEntry]) -> pl.DataFrame:
    """Insert the parsed chart data into polars dataframe"""
    web_chart = pl.DataFrame(chart_entries)
    web_chart = web_chart.drop('id')
    return web_chart


@task
def filter_new_charts(
    existing_charts: pl.DataFrame, web_charts: pl.DataFrame
) -> pl.DataFrame:
    """Anti-join the web charts with existing chart data
    to return only new chart data"""
    new_chart_data = web_charts.join(
        other=existing_charts,
        on=['song_id', 'chart_type', 'week'],
        how='anti',
    )
    return new_chart_data


@task
def insert_chart_data_into_db(new_chart_data: pl.DataFrame) -> None:
    """Write the dataframe of new chart data into database"""
    with duckdb.connect(config.DB_PATH) as conn:
        conn.execute(
            """-- sql
            insert into charts by name (
                select * from new_chart_data
            )
            """
        )


@flow
def update_charts_flow(soup: BeautifulSoup) -> None:
    """Main flow to fetch the webpage, parse html, extract chart entries' elements
    and parse chart entries' data, load existing chart data to filter only new data
    and finaly insert chart data into database."""
    chart_soup = extract_chart_elements(soup=soup)
    charts = parse_chart_data(soup=chart_soup)
    existing_chart_data = load_existing_chart_data()
    web_charts = create_web_charts_df(charts)
    new_charts = filter_new_charts(existing_chart_data, web_charts)
    insert_chart_data_into_db(new_charts)
