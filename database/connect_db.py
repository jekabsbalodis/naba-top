import anodb

from config import config

db = anodb.DB(
    db='duckdb',
    conn=str(config.DB_PATH),
    queries=str(config.QUERIES_PATH),
)
