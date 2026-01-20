from modulefinder import packagePathMap
import httpx
from prefect import task
from prefect.tasks import exponential_backoff
from pydantic import EmailStr, HttpUrl
from bs4 import BeautifulSoup, ResultSet, Tag
from config import config
from models import ChartEntry, Song


@task(
    retries=10,
    retry_delay_seconds=exponential_backoff(backoff_factor=3),
)
def read_webpage(url: HttpUrl, email: EmailStr) -> httpx.Response:
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
def parse_webpage(res: httpx.Response) -> BeautifulSoup:
    """Parse the returned response HTML text"""
    return BeautifulSoup(res.text, 'lxml')


@task
def extract_charts(soup: BeautifulSoup) -> ResultSet[Tag]:
    """Return the elements for the top10 and top25 charts"""
    return soup.select('.songsList')


@task
def parse_charts(soup: ResultSet[Tag]) -> list[Song]:
    """Parse the part of the html page that contains information about top10 chart"""
    top10_soup = soup[0].select('.songLine')

    song_list: list[Song] = []
    top10: list[ChartEntry]

    for entry in top10_soup:
        if entry.select_one('.songName') and entry.select_one('.songPlace') is None:
            continue
        web_songname = entry.select_one('.songName').string
        place = entry.select_one('.songPlace').string
        artist, song_name = web_songname.split(' - ')[0], web_songname.split(' - ')[1]
        song = Song(artist=artist, song_name=song_name, web_songname=web_songname)
        chart_entry = ChartEntry()
        song_list.append(song)
