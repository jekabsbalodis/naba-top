from unittest.mock import MagicMock, patch

import httpx
import pytest
from bs4 import BeautifulSoup
from pydantic import HttpUrl

from flows.shared_tasks import fetch_webpage, parse_html, upload_data


@pytest.fixture
def sample_html() -> str:
    return """
    <html>
      <head><title>Test Page</title></head>
      <body><p class="content">Hello, world!</p></body>
    </html>
    """


@pytest.fixture
def mock_response(sample_html, flow_url) -> httpx.Response:
    return httpx.Response(
        status_code=200,
        text=sample_html,
        request=httpx.Request('GET', flow_url),
    )


class TestFetchWebpage:
    @pytest.fixture(autouse=True)
    def mock_client(self, mock_response):
        with patch('flows.shared_tasks.httpx.Client') as mock_client_cls:
            client = MagicMock()
            client.__enter__ = MagicMock(return_value=client)
            client.__exit__ = MagicMock(return_value=False)
            client.get.return_value = mock_response
            mock_client_cls.return_value = client
            self.mock_client_cls = mock_client_cls
            yield client

    def test_returns_httpx_response(self, flow_url, flow_email):
        result = fetch_webpage.fn(flow_url, flow_email)
        assert isinstance(result, httpx.Response)
        assert result.status_code == 200

    def test_sends_correct_headers(self, flow_url, flow_email):
        fetch_webpage.fn(flow_url, flow_email)
        kwargs = self.mock_client_cls.call_args.kwargs
        assert str(flow_email) in kwargs['headers']['user-agent']
        assert 'text/html' == kwargs['headers']['accept']

    def test_get_called_with_stringified_url(self, mock_client, flow_url, flow_email):
        fetch_webpage.fn(flow_url, flow_email)
        mock_client.get.assert_called_once_with(url=HttpUrl(flow_url).unicode_string())

    def test_raises_on_4xx(self, flow_url, flow_email):
        error_response = httpx.Response(
            status_code=404,
            request=httpx.Request('GET', flow_url),
        )
        with patch('flows.shared_tasks.httpx.Client') as mock_client_cls:
            client = MagicMock()
            client.__enter__ = MagicMock(return_value=client)
            client.__exit__ = MagicMock(return_value=False)
            client.get.return_value = error_response
            mock_client_cls.return_value = client

            with pytest.raises(httpx.HTTPStatusError):
                fetch_webpage.fn(flow_url, flow_email)

    def test_raises_on_5xx(self, flow_url, flow_email):
        error_response = httpx.Response(
            status_code=503,
            request=httpx.Request('GET', flow_url),
        )
        with patch('flows.shared_tasks.httpx.Client') as mock_client_cls:
            client = MagicMock()
            client.__enter__ = MagicMock(return_value=client)
            client.__exit__ = MagicMock(return_value=False)
            client.get.return_value = error_response
            mock_client_cls.return_value = client

            with pytest.raises(httpx.HTTPStatusError):
                fetch_webpage.fn(flow_url, flow_email)


class TestParseHtml:
    def test_returns_beautifulsoup(self, mock_response):
        result = parse_html.fn(mock_response)
        assert isinstance(result, BeautifulSoup)

    def test_uses_lxml_parser(self, mock_response):
        soup = parse_html.fn(mock_response)
        # lxml sets the builder name to 'lxml'
        assert 'lxml' in soup.builder.NAME

    def test_empty_html(self, flow_url):
        empty_response = httpx.Response(
            status_code=200,
            text='',
            request=httpx.Request('GET', flow_url),
        )
        soup = parse_html.fn(empty_response)
        assert isinstance(soup, BeautifulSoup)

    def test_malformed_html(self, flow_url):
        """parse_html handles malformed HTML without raising."""
        bad_response = httpx.Response(
            status_code=200,
            text='<div><p>Unclosed',
            request=httpx.Request('GET', flow_url),
        )
        soup = parse_html.fn(bad_response)
        assert soup.find('p') is not None


class TestUploadData:
    @pytest.fixture(autouse=True)
    def mock_s3(self):
        with patch(
            'flows.shared_tasks.s3_connection', return_value=MagicMock()
        ) as mock:
            yield mock

    def test_executes_copy_statements(self, db_path, s3_config, mock_s3):
        upload_data.fn(db_path, s3_config)
        conn = mock_s3.return_value.__enter__.return_value
        conn.execute.assert_called_once()

    def test_copies_all_tables(self, db_path, s3_config, mock_s3):
        upload_data.fn(db_path, s3_config)
        conn = mock_s3.return_value.__enter__.return_value
        sql = conn.execute.call_args.args[0]
        for table in ['all_songs_ranked', 'top10', 'top25', 'charts', 'songs']:
            assert table in sql

    def test_passes_correct_args_to_s3_connection(self, db_path, s3_config, mock_s3):
        upload_data.fn(db_path, s3_config)
        mock_s3.assert_called_once_with(db_path=db_path, s3_config=s3_config)

    def test_raises_on_connection_failure(self, db_path, s3_config, mock_s3):
        mock_s3.return_value.__enter__.side_effect = Exception('connection failed')
        with pytest.raises(Exception, match='connection failed'):
            upload_data.fn(db_path, s3_config)
