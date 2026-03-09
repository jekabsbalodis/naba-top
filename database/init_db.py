"""Module for database initialization.

Creates and initializes DuckDB database schema - tables, sequences, indexes and views.
"""

import duckdb


def init_db(db_path: str) -> None:
    """Create the database sequences and tables.

    Args:
      db_path: The path to the database file.
      Use ':memory:' to initialize an in-memory database.

    """
    with duckdb.connect(db_path) as conn:
        conn.execute("""-- sql
        -- Create the table where the songs will be stored
        CREATE SEQUENCE IF NOT EXISTS song_id_sequence START 1;

        CREATE TABLE IF NOT EXISTS songs (
          id UINT16 PRIMARY KEY DEFAULT nextval('song_id_sequence'),
          artist VARCHAR,
          song_name VARCHAR,
          web_songname VARCHAR UNIQUE,
          UNIQUE (artist, song_name)
        );

        CREATE INDEX IF NOT EXISTS idx_songs_artist ON songs (artist);

        -- Create the table where the charts will be stored
        CREATE SEQUENCE IF NOT EXISTS chart_id_sequence START 1;

        CREATE TABLE IF NOT EXISTS charts (
          id UINT32 PRIMARY KEY DEFAULT nextval('chart_id_sequence'),
          song_id UINT16 NOT NULL,
          chart_type VARCHAR NOT NULL CHECK (chart_type IN ('top10', 'top25')),
          place UINT8,
          week DATE NOT NULL,
          is_new_entry BOOL DEFAULT false,
          FOREIGN KEY (song_id) REFERENCES songs (id),
          UNIQUE (song_id, week, chart_type)
        );

        CREATE INDEX IF NOT EXISTS idx_charts_week ON charts (week);

        CREATE INDEX IF NOT EXISTS idx_charts_type_week ON charts (chart_type, week);

        CREATE INDEX IF NOT EXISTS idx_charts_song_id ON charts (song_id);

        -- Create a view to display TOP 10
        create view if not exists top10 as
        select
          c.week,
          s.artist,
          s.song_name,
          c.place,
          c.is_new_entry
        from
          charts c
          join songs s on c.song_id = s.id
        where
          c.chart_type = 'top10'
        order by
          c.week desc,
          c.place asc;

        -- Create a view to display TOP 25
        create view if not exists top25 as
        select
          c.week,
          s.artist,
          s.song_name,
          c.place,
          c.is_new_entry
        from
          charts c
          join songs s on c.song_id = s.id
        where
          c.chart_type = 'top25'
        order by
          c.week desc,
          c.place asc;

        -- Create a view for all songs ranked
        CREATE view if not exists all_songs_ranked AS
        WITH
          song_scores AS (
            SELECT
              c.song_id,
              COUNT(*) as weeks_in_chart,
              FSUM(
                CASE
                  WHEN c.chart_type = 'top10'
                  AND c.place IS NOT NULL THEN (11 - c.place)
                  WHEN c.chart_type = 'top25' THEN ((26 - c.place) / 2.5)
                  ELSE 0
                END
              ) as normalized_points,
              FSUM(
                CASE
                  WHEN c.chart_type = 'top10'
                  AND c.place IS NOT NULL THEN (11 - c.place)
                  WHEN c.chart_type = 'top25' THEN (26 - c.place)
                  ELSE 0
                END
              ) as raw_points,
              case
                when bool_or(c.chart_type = 'top10') then 'top10'
                else 'top25'
              end as chart_type
            FROM
              charts c
            WHERE
              c.place IS NOT NULL
            GROUP BY
              c.song_id
          )
        SELECT
          rank() OVER (ORDER BY ss.normalized_points DESC,
                                ss.weeks_in_chart ASC,
                                ss.raw_points DESC) AS place,
          s.id as song_id,
          s.artist,
          s.song_name,
          ss.weeks_in_chart,
          ss.raw_points,
          ss.normalized_points as score,
          ss.chart_type
        FROM
          song_scores ss
          JOIN songs s ON ss.song_id = s.id
        ORDER BY
          score DESC;
        """)


if __name__ == '__main__':
    import typer

    typer.run(init_db)
