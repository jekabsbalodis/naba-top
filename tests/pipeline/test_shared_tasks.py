import json
from unittest.mock import MagicMock

import httpx2 as httpx
import polars as pl
import pytest

from models import S3Config
from pipeline.shared_tasks import fetch_webpage, upload_data
from tests.helpers import make_sqlmesh_db

URL = 'https://www.example.com/charts'
EMAIL = 'test@example.com'


# --- fetch_webpage ----------------------------------------------------------


def test_fetch_returns_the_page(web):
    web.html = '<html>hello</html>'
    response = fetch_webpage(URL, EMAIL)
    assert response.status_code == 200
    assert 'hello' in response.text


def test_fetch_asks_for_the_given_url(web):
    fetch_webpage(URL, EMAIL)
    assert [str(r.url) for r in web.requests] == [URL]


def test_fetch_identifies_itself_with_the_email(web):
    fetch_webpage(URL, EMAIL)
    headers = web.requests[0].headers
    assert EMAIL in headers['user-agent']
    assert headers['accept'] == 'text/html'


@pytest.mark.parametrize('status', [404, 503])
def test_fetch_raises_on_error_status(web, status):
    web.status = status
    with pytest.raises(httpx.HTTPStatusError):
        fetch_webpage(URL, EMAIL)


# --- upload_data ------------------------------------------------------------


def test_upload_to_local_dir_exports_tables_and_a_manifest(tmp_path):
    source = str(tmp_path / 'sqlmesh.duckdb')
    make_sqlmesh_db(source)
    target = tmp_path / 'upload'

    upload_data(source, local_dir=target)

    assert pl.read_parquet(target / 'mart' / 'top10.parquet').rows() == [(1, 'Song')]
    manifest = json.loads((target / 'manifest.json').read_text())
    # `sqlmesh__mart` is internal to SQLMesh and must not be uploaded
    assert [(m['schema'], m['view']) for m in manifest] == [('mart', 'top10')]


def test_upload_needs_a_destination(tmp_path):
    with pytest.raises(ValueError, match='s3_config or local_dir'):
        upload_data(str(tmp_path / 'sqlmesh.duckdb'))


def test_upload_to_s3_uses_the_given_credentials(tmp_path, monkeypatch):
    s3 = S3Config(key_id='id', secret='secret', endpoint='s3.example.com', region='r')
    connection = MagicMock()
    connection.execute.return_value.fetchall.return_value = [('mart', 'top10')]
    s3_connection = MagicMock()
    s3_connection.return_value.__enter__.return_value = connection
    monkeypatch.setattr('pipeline.shared_tasks.s3_connection', s3_connection)

    upload_data(str(tmp_path / 'sqlmesh.duckdb'), s3_config=s3)

    s3_connection.assert_called_once_with(s3_config=s3)
    sql = ' '.join(str(call.args[0]) for call in connection.execute.call_args_list)
    assert 's3://naba-top/mart/top10.parquet' in sql
    assert 's3://naba-top/manifest.json' in sql
