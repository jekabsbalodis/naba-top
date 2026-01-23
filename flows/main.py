from prefect import flow
from pydantic import EmailStr, HttpUrl

from config import config
from flows.shared_tasks import fetch_webpage, parse_html
from flows.update_charts import update_charts_flow
from flows.update_songs import update_songs_flow


@flow
def main_flow(
    url: HttpUrl = config.FLOW_URL,
    email: EmailStr = config.FLOW_EMAIL,
) -> None:
    """Main flow that orchestrates updating songs and charts table in db"""
    response = fetch_webpage(url, email)
    soup = parse_html(response)

    update_songs_flow(soup)
    update_charts_flow(soup)


if __name__ == '__main__':
    main_flow.deploy(name='naba-top-scrape')
