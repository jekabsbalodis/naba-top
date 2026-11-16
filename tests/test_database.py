from dataclasses import fields

import duckdb

from database.init_db import init_db
from models import ChartEntry, Song


def column_names(db_path: str, table: str) -> set[str]:
    with duckdb.connect(db_path) as conn:
        return {row[0] for row in conn.execute(f'describe {table}').fetchall()}


def test_tables_have_the_columns_of_the_models(db_path):
    assert column_names(db_path, 'songs') == {f.name for f in fields(Song)}
    assert column_names(db_path, 'charts') == {f.name for f in fields(ChartEntry)}


def test_init_db_can_run_twice_without_losing_data(db_path):
    with duckdb.connect(db_path) as conn:
        conn.execute(
            'insert into songs (artist, song_name, web_songname) values (?, ?, ?)',
            ['a', 's', 'a - s'],
        )
    init_db(db_path)
    with duckdb.connect(db_path) as conn:
        assert conn.execute('select count(*) from songs').fetchone() == (1,)
