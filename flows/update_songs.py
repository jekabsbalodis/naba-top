import duckdb
import polars as pl
from bs4 import BeautifulSoup, ResultSet, Tag
from prefect import flow, task

from config import config
from models import Song


@task
def extract_song_elements(soup: BeautifulSoup) -> ResultSet[Tag]:
    """Return the elements for the songs"""
    return soup.css.select('.songName')


@task
def parse_song_data(soup: ResultSet[Tag]) -> list[Song]:
    """Parse the part of the html page that contains information
    about songs - artist and song name"""
    songs = soup

    song_list: list[Song] = []

    for song in songs:
        web_songname: str = song.text
        artist, song_name = web_songname.split(' - ', 1)
        song_list.append(
            Song(
                artist=artist,
                song_name=song_name,
                web_songname=web_songname,
            )
        )

    return song_list


@task
def create_songs_df(song_list: list[Song]) -> pl.DataFrame:
    """Insert the parsed song values into polars dataframe"""
    web_songs = pl.DataFrame(song_list)
    web_songs = web_songs.drop('id')
    return web_songs


@task
def insert_songs_into_db(new_songs: pl.DataFrame) -> None:
    """Write the dataframe of new songs into database"""
    with duckdb.connect(config.DB_PATH) as conn:
        conn.execute(
            """-- sql
            insert or ignore into songs (artist, song_name, web_songname) (
                select * from new_songs
            )
            """
        )


@flow
def update_songs_flow(soup: BeautifulSoup) -> None:
    """Main flow to fetch the webpage, parse html, extract song elements
    and parse song data, load existing songs to filter only new songs
    and finaly insert songs into database."""
    song_soup = extract_song_elements(soup=soup)
    songs = parse_song_data(soup=song_soup)
    songs_df = create_songs_df(songs)
    insert_songs_into_db(songs_df)
