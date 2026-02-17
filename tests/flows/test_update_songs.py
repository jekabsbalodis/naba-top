import duckdb
import polars as pl
import pytest
from bs4 import BeautifulSoup, ResultSet

from config import config
from database.init_db import init_db
from flows.update_songs import (
    create_songs_df,
    extract_song_elements,
    insert_songs_into_db,
    parse_song_data,
)
from models import Song


@pytest.fixture
def sample_html() -> BeautifulSoup:
    html = """
    <html><body>
        <span class="songName">Artist One - Song One</span>
        <span class="songName">Artist Two - Song Two</span>
        <span class="songName">Artist Three - Song Three</span>
    </body></html>
    """
    return BeautifulSoup(html, 'lxml')


@pytest.fixture
def song_elements(sample_html) -> ResultSet:
    return sample_html.css.select('.songName')


@pytest.fixture
def song_list() -> list[Song]:
    return [
        Song(
            artist='Artist One',
            song_name='Song One',
            web_songname='Artist One - Song One',
        ),
        Song(
            artist='Artist Two',
            song_name='Song Two',
            web_songname='Artist Two - Song Two',
        ),
        Song(
            artist='Artist Three',
            song_name='Song Three',
            web_songname='Artist Three - Song Three',
        ),
    ]


@pytest.fixture
def songs_df(song_list) -> pl.DataFrame:
    df = pl.DataFrame(song_list)
    return df.drop('id')


@pytest.fixture(autouse=True)
def setup_db():
    """Initialize test DB before each test and clean songs table after"""
    init_db()
    yield
    with duckdb.connect(config.DB_PATH) as conn:
        conn.execute('delete from songs')


class TestExtractSongElements:
    def test_returns_result_set(self, sample_html):
        result = extract_song_elements.fn(sample_html)
        assert isinstance(result, ResultSet)

    def test_returns_correct_count(self, sample_html):
        result = extract_song_elements.fn(sample_html)
        assert len(result) == 3

    def test_returns_correct_text(self, sample_html):
        result = extract_song_elements.fn(sample_html)
        texts = [el.text for el in result]
        assert 'Artist One - Song One' in texts

    def test_empty_page_returns_empty_result(self):
        soup = BeautifulSoup('<html><body></body></html>', 'lxml')
        result = extract_song_elements.fn(soup)
        assert len(result) == 0


class TestParseSongData:
    def test_returns_list_of_songs(self, song_elements):
        result = parse_song_data.fn(song_elements)
        assert isinstance(result, list)
        assert all(isinstance(s, Song) for s in result)

    def test_returns_correct_count(self, song_elements):
        result = parse_song_data.fn(song_elements)
        assert len(result) == 3

    def test_parses_artist_and_song_name(self, song_elements):
        result = parse_song_data.fn(song_elements)
        assert result[0].artist == 'Artist One'
        assert result[0].song_name == 'Song One'
        assert result[0].web_songname == 'Artist One - Song One'

    def test_song_name_with_dash(self):
        """Song names that contain ' - ' should split on first occurrence only"""
        html = '<span class="songName">Artist - Song - With - Dashes</span>'
        soup = BeautifulSoup(html, 'lxml')
        elements = soup.css.select('.songName')
        result = parse_song_data.fn(elements)
        assert result[0].artist == 'Artist'
        assert result[0].song_name == 'Song - With - Dashes'


class TestCreateSongsDf:
    def test_returns_polars_dataframe(self, song_list):
        result = create_songs_df.fn(song_list)
        assert isinstance(result, pl.DataFrame)

    def test_id_column_is_dropped(self, song_list):
        result = create_songs_df.fn(song_list)
        assert 'id' not in result.columns

    def test_correct_columns(self, song_list):
        result = create_songs_df.fn(song_list)
        assert set(result.columns) == {'artist', 'song_name', 'web_songname'}

    def test_correct_row_count(self, song_list):
        result = create_songs_df.fn(song_list)
        assert len(result) == 3


class TestInsertSongsIntoDb:
    def test_inserts_songs(self, songs_df):
        insert_songs_into_db.fn(songs_df)
        with duckdb.connect(config.DB_PATH) as conn:
            count = conn.sql('select count(*) from songs').fetchone()
        assert count is not None
        assert count[0] == 3

    def test_ignores_duplicates(self, songs_df):
        insert_songs_into_db.fn(songs_df)
        insert_songs_into_db.fn(songs_df)
        with duckdb.connect(config.DB_PATH) as conn:
            count = conn.sql('select count(*) from songs').fetchone()
        assert count is not None
        assert count[0] == 3

    def test_inserts_correct_values(self, songs_df):
        insert_songs_into_db.fn(songs_df)
        with duckdb.connect(config.DB_PATH) as conn:
            row = conn.sql(
                """-- sql
                select
                    artist,
                    song_name,
                    web_songname
                from
                    songs
                where
                    artist = 'Artist One'
                """
            ).fetchone()
        assert row == ('Artist One', 'Song One', 'Artist One - Song One')
