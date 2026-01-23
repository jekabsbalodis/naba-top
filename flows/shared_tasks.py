import httpx
from bs4 import BeautifulSoup
from prefect import task
from prefect.tasks import exponential_backoff
from pydantic import EmailStr, HttpUrl


@task(
    retries=10,
    retry_delay_seconds=exponential_backoff(backoff_factor=3),
)
def fetch_webpage(url: HttpUrl, email: EmailStr) -> httpx.Response:
    """Read naba.lv webpage and return httpx response"""
    headers = {
        'user-agent': f'python-httpx {str(email)}',
        'accept': 'text/html',
    }
    with httpx.Client(headers=headers) as client:
        response = client.get(url=str(url))
    response.raise_for_status()
    return response


@task
def parse_html(res: httpx.Response) -> BeautifulSoup:
    """Parse the returned response HTML text"""
    return BeautifulSoup(res.text, 'lxml')
