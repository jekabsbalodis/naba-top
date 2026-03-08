from dataclasses import fields

import duckdb

from database.init_db import init_db
from models import ChartEntry, RankedSongEntry, Song, TopEntry


def test_column_names_match_models(db_path):
    init_db(db_path)
    with duckdb.connect(db_path) as conn:
        table_songs = conn.sql('describe songs;').fetchall()
        table_charts = conn.sql('describe charts;').fetchall()
        view_top10 = conn.sql('describe top10;').fetchall()
        view_top25 = conn.sql('describe top25;').fetchall()
        view_all_songs_ranked = conn.sql('describe all_songs_ranked;').fetchall()

    actual_songs = [row[0] for row in table_songs]
    expected_songs = [field.name for field in fields(Song)]

    actual_charts = [row[0] for row in table_charts]
    expected_charts = [field.name for field in fields(ChartEntry)]

    actual_top10 = [row[0] for row in view_top10]
    expected_top10 = [field.name for field in fields(TopEntry)]

    actual_top25 = [row[0] for row in view_top25]
    expected_top25 = [field.name for field in fields(TopEntry)]

    actual_all_songs_ranked = [row[0] for row in view_all_songs_ranked]
    expected_all_songs_ranked = [field.name for field in fields(RankedSongEntry)]

    assert set(actual_songs) == set(expected_songs), (
        f'\nSong columns mismatch.\n\
        Columns in db - {actual_songs}\n\
        Values in model - {expected_songs}'
    )

    assert set(actual_charts) == set(expected_charts), (
        f'\nChart entry columns mismatch.\n\
        Columns in db - {actual_charts}\n\
        Values in model - {expected_charts}'
    )

    assert set(actual_top10) == set(expected_top10), (
        f'\nTop 10 entry columns mismatch.\n\
        Columns in db - {actual_top10}\n\
        Values in model - {expected_top10}'
    )

    assert set(actual_top25) == set(expected_top25), (
        f'\nTop 10 entry columns mismatch.\n\
        Columns in db - {actual_top25}\n\
        Values in model - {expected_top25}'
    )

    assert set(actual_all_songs_ranked) == set(expected_all_songs_ranked), (
        f'\nTop 10 entry columns mismatch.\n\
        Columns in db - {actual_all_songs_ranked}\n\
        Values in model - {expected_all_songs_ranked}'
    )
