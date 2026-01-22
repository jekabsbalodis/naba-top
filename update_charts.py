import duckdb
import httpx
import polars as pl
from bs4 import BeautifulSoup, ResultSet, Tag
from prefect import flow, task
from prefect.tasks import exponential_backoff
from pydantic import EmailStr, HttpUrl

from config import config
from models import Song


@task(
    retries=10,
    retry_delay_seconds=exponential_backoff(backoff_factor=3),
)
def fetch_webpage(url: HttpUrl, email: EmailStr) -> httpx.Response:
    """Read naba.lv webpage and return httpx response"""
    headers = {
        'user-agent': f'python-httpx {str(email)}',
        'accept': 'text/html',
    }
    with httpx.Client(headers=headers) as client:
        response = client.get(url=str(url))
    response.raise_for_status()
    return response


@task
def parse_html(res: httpx.Response) -> BeautifulSoup:
    """Parse the returned response HTML text"""
    return BeautifulSoup(res.text, 'lxml')


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
def load_existing_songs() -> pl.DataFrame:
    """Query database for records already available"""
    with duckdb.connect(config.DB_PATH) as conn:
        df = conn.sql('select * from songs').pl()

    df.drop('id')
    return df


@task
def create_web_songs_df(song_list: list[Song]) -> pl.DataFrame:
    """Insert the parsed song values into polars dataframe"""
    web_songs = pl.DataFrame(song_list)
    web_songs = web_songs.drop('id')
    return web_songs


@task
def filter_new_songs(
    existing_songs: pl.DataFrame, web_songs: pl.DataFrame
) -> pl.DataFrame:
    """Anti-join the web songs with existing songs to return only new songs"""
    new_songs = web_songs.join(
        other=existing_songs,
        on=['web_songname'],
        how='anti',
    )
    return new_songs


@task
def insert_songs_into_db(new_songs: pl.DataFrame) -> None:
    """Write the dataframe of new songs into database"""
    with duckdb.connect(config.DB_PATH) as conn:
        conn.execute(
            """-- sql
            insert into songs by name (
                select * from new_songs
            )
            """
        )


@flow
def update_charts(url: HttpUrl, email: EmailStr) -> None:
    """Main flow to fetch the webpage, parse html, extract song elements
    and parse song data, load existing songs to filter only new songs
    and finaly insert songs into database."""
    res = fetch_webpage(url=config.FLOW_URL, email=config.FLOW_EMAIL)
    soup = parse_html(res=res)
    song_soup = extract_song_elements(soup=soup)
    songs = parse_song_data(soup=song_soup)
    existing_songs = load_existing_songs()
    web_songs = create_web_songs_df(songs)
    new_songs = filter_new_songs(existing_songs, web_songs)
    insert_songs_into_db(new_songs)
