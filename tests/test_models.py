from dataclasses import fields

import duckdb

from config import config
from database.init_db import init_db
from models import ChartEntry, Song


def test_column_names_match_models():
    init_db()
    with duckdb.connect(config.DB_PATH) as conn:
        r_songs = conn.sql('describe songs;').fetchall()
        r_charts = conn.sql('describe charts;').fetchall()
    actual_songs = [row[0] for row in r_songs]
    expected_songs = [field.name for field in fields(Song)]
    actual_charts = [row[0] for row in r_charts]
    expected_charts = [field.name for field in fields(ChartEntry)]
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
