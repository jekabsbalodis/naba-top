import duckdb
import polars as pl
import pytest
from bs4 import BeautifulSoup

from models import Song
from pipeline.update_songs import (
    create_songs_df,
    extract_song_elements,
    insert_songs_into_db,
    parse_song_data,
)

HTML = """
<html><body>
  <span class="songName">Artist One - Song One</span>
  <span class="songName">Artist Two - Song Two</span>
</body></html>
"""


@pytest.fixture
def song_elements():
    return extract_song_elements(BeautifulSoup(HTML, 'lxml'))


@pytest.fixture
def songs() -> list[Song]:
    return [
        Song('Artist One', 'Song One', 'Artist One - Song One'),
        Song('Artist Two', 'Song Two', 'Artist Two - Song Two'),
    ]


def test_extract_finds_all_song_elements(song_elements):
    assert [el.text for el in song_elements] == [
        'Artist One - Song One',
        'Artist Two - Song Two',
    ]


def test_extract_from_empty_page_finds_nothing():
    assert len(extract_song_elements(BeautifulSoup('<html></html>', 'lxml'))) == 0


def test_parse_splits_artist_and_song(song_elements):
    first = parse_song_data(song_elements)[0]
    assert first == Song('Artist One', 'Song One', 'Artist One - Song One')


def test_parse_splits_only_on_the_first_dash():
    html = '<span class="songName">Artist - Song - With - Dashes</span>'
    elements = extract_song_elements(BeautifulSoup(html, 'lxml'))
    song = parse_song_data(elements)[0]
    assert (song.artist, song.song_name) == ('Artist', 'Song - With - Dashes')


def test_parse_fails_on_a_name_without_a_separator():
    html = '<span class="songName">No separator here</span>'
    elements = extract_song_elements(BeautifulSoup(html, 'lxml'))
    with pytest.raises(ValueError):
        parse_song_data(elements)


def test_songs_df_has_no_id_column(songs):
    df = create_songs_df(songs)
    assert set(df.columns) == {'artist', 'song_name', 'web_songname'}
    assert len(df) == 2


def test_insert_adds_the_songs(songs, db_path):
    insert_songs_into_db(create_songs_df(songs), db_path)
    with duckdb.connect(db_path) as conn:
        rows = conn.execute(
            'select artist, song_name, web_songname from songs order by id'
        )
        assert rows.fetchall() == [
            ('Artist One', 'Song One', 'Artist One - Song One'),
            ('Artist Two', 'Song Two', 'Artist Two - Song Two'),
        ]


def test_insert_ignores_songs_that_exist(songs, db_path):
    df: pl.DataFrame = create_songs_df(songs)
    insert_songs_into_db(df, db_path)
    insert_songs_into_db(df, db_path)
    with duckdb.connect(db_path) as conn:
        assert conn.execute('select count(*) from songs').fetchone() == (2,)
