"""Database initialization module for naba-top application.

Provides functions to create and initialize the DuckDB database schema
including tables, indexes, sequences and views for music chart data.
"""

from pathlib import Path

import duckdb

SCHEMA: str = Path(__file__).with_name('schema.sql').read_text()


def init_db(db_path: str) -> None:
    """Initialize the database schema with tables, indexes and views.

    Creates the complete database structure including:
    - songs table with artist and song information
    - charts table with chart positions and types (top10/top25)
    - indexes for efficient querying

    Args:
        db_path: Path to the DuckDB database file or ':memory:' for in-memory database.

    The function creates:
    - song_id_sequence and chart_id_sequence for auto-incrementing IDs.
    - songs table with artist, song_name, and web_songname fields.
    - charts table with song_id foreign key, chart_type, place, week, and is_new_entry.
    - Multiple indexes for performance optimization.

    """
    with duckdb.connect(db_path) as conn:
        conn.execute(SCHEMA)


if __name__ == '__main__':
    import typer

    typer.run(init_db)
