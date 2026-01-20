from dataclasses import fields

from database.connect_db import db
from models import Song


def test_column_names_match_models():
    db.create_songs_table()
    actual_songs = [row[0] for row in db.describe_songs()]
    expected_songs = [field.name for field in fields(Song)]
    assert set(actual_songs) == set(expected_songs), (
        f'Song columns mismatch.\n\
        Columns in db - {actual_songs}\n\
        Values in model - {expected_songs}'
    )
