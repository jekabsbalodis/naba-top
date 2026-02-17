from datetime import date

import duckdb
import polars as pl
import pytest
from bs4 import BeautifulSoup, ResultSet

from config import config
from database.init_db import init_db
from flows.update_charts import (
    create_charts_df,
    extract_chart_elements,
    insert_chart_data_into_db,
    parse_chart_data,
)
from models import ChartEntry, ChartType

SAMPLE_HTML = """
<html><body>
<div class="songsList">
    <form>
        <label class="songLine">
            <div class="leftLine">
                <div class="songPlace">1</div>
                <div class="songName">Artist One - Song One</div>
            </div>
        </label>
        <div class="naba-top-song">
            <div class="song_vote_info">
                <div class="place_previous">2</div>
            </div>
        </div>

        <label class="songLine">
            <div class="leftLine">
                <div class="songPlace">j</div>
                <div class="songName">Artist Two - Song Two</div>
            </div>
        </label>
        <div class="naba-top-song">
            <div class="song_vote_info">
                <div class="place_previous">j</div>
            </div>
        </div>

        <div class="newsCard__date songListDate">13.02.2026</div>
    </form>
</div>

<div class="songsList">
    <form>
        <label class="songLine">
            <div class="leftLine">
                <div class="songPlace">1</div>
                <div class="songName">Artist Three - Song Three</div>
            </div>
        </label>
        <div class="naba-top-song">
            <div class="song_vote_info">
                <div class="place_previous">3</div>
            </div>
        </div>

        <label class="songLine">
            <div class="leftLine">
                <div class="songPlace">j</div>
                <div class="songName">Artist Four - Song Four</div>
            </div>
        </label>
        <div class="naba-top-song">
            <div class="song_vote_info">
                <div class="place_previous">j</div>
            </div>
        </div>

        <div class="newsCard__date songListDate">13.02.2026</div>
    </form>
</div>
</body></html>
"""

CHART_SONGS = [
    ('Artist One', 'Song One', 'Artist One - Song One'),
    ('Artist Two', 'Song Two', 'Artist Two - Song Two'),
    ('Artist Three', 'Song Three', 'Artist Three - Song Three'),
    ('Artist Four', 'Song Four', 'Artist Four - Song Four'),
]


@pytest.fixture
def sample_soup() -> BeautifulSoup:
    return BeautifulSoup(SAMPLE_HTML, 'lxml')


@pytest.fixture
def chart_elements(sample_soup) -> ResultSet:
    return sample_soup.css.select('.songsList')


@pytest.fixture(autouse=True)
def setup_db():
    """Initialize DB, pre-insert songs needed by parse_chart_data, clean up after"""
    init_db()
    with duckdb.connect(config.DB_PATH) as conn:
        conn.executemany(
            """--sql
            insert or ignore into songs
                (artist, song_name, web_songname)
            values
                (?, ?, ?)
            """,
            CHART_SONGS,
        )
    yield
    with duckdb.connect(config.DB_PATH) as conn:
        conn.execute('delete from charts')
        conn.execute('delete from songs')


@pytest.fixture
def chart_entries(chart_elements) -> list[ChartEntry]:
    return parse_chart_data.fn(chart_elements)


@pytest.fixture
def charts_df(chart_entries) -> pl.DataFrame:
    return create_charts_df.fn(chart_entries)


class TestExtractChartElements:
    def test_returns_result_set(self, sample_soup):
        result = extract_chart_elements.fn(sample_soup)
        assert isinstance(result, ResultSet)

    def test_returns_two_charts(self, sample_soup):
        result = extract_chart_elements.fn(sample_soup)
        assert len(result) == 2

    def test_empty_page_returns_empty(self):
        soup = BeautifulSoup('<html><body></body></html>', 'lxml')
        result = extract_chart_elements.fn(soup)
        assert len(result) == 0


class TestParseChartData:
    def test_returns_list_of_chart_entries(self, chart_entries):
        assert isinstance(chart_entries, list)
        assert all(isinstance(e, ChartEntry) for e in chart_entries)

    def test_returns_correct_total_count(self, chart_entries):
        # 2 songs in top10 + 2 songs in top25
        assert len(chart_entries) == 4

    def test_top10_and_top25_chart_types(self, chart_entries):
        types = [e.chart_type for e in chart_entries]
        assert types.count(ChartType.TOP10) == 2
        assert types.count(ChartType.TOP25) == 2

    def test_parses_numeric_place(self, chart_entries):
        top10_entries = [e for e in chart_entries if e.chart_type == ChartType.TOP10]
        numbered = next(e for e in top10_entries if e.place is not None)
        assert numbered.place == 1

    def test_j_place_becomes_none(self, chart_entries):
        top10_entries = [e for e in chart_entries if e.chart_type == ChartType.TOP10]
        new_entry = next(e for e in top10_entries if e.place is None)
        assert new_entry.place is None

    def test_parses_week_date(self, chart_entries):
        assert all(e.week == date(2026, 2, 13) for e in chart_entries)

    def test_is_new_entry_true_when_not_in_charts(self, chart_elements):
        """Songs not yet in charts should be marked as new entries"""
        result = parse_chart_data.fn(chart_elements)
        assert all(e.is_new_entry is True for e in result)

    def test_is_new_entry_true_when_place_is_none(self, chart_elements):
        first_run = parse_chart_data.fn(chart_elements)
        df = create_charts_df.fn(first_run)
        insert_chart_data_into_db.fn(df)

        second_run = parse_chart_data.fn(chart_elements)
        j_entries = [e for e in second_run if e.place is None]
        assert len(j_entries) > 0
        assert all(e.is_new_entry is True for e in j_entries)

    def test_is_new_entry_false_when_in_charts_and_has_place(self, chart_elements):
        first_run = parse_chart_data.fn(chart_elements)
        df = create_charts_df.fn(first_run)
        insert_chart_data_into_db.fn(df)

        second_run = parse_chart_data.fn(chart_elements)
        returning_entries = [e for e in second_run if e.place is not None]
        assert len(returning_entries) > 0
        assert all(e.is_new_entry is False for e in returning_entries)

    def test_song_id_resolved_from_db(self, chart_entries):
        assert all(isinstance(e.song_id, int) for e in chart_entries)
        assert all(e.song_id > 0 for e in chart_entries)

    def test_raises_when_no_date_tag(self):
        """LookupError raised when songListDate is missing"""
        html = """
        <html><body>
        <div class="songsList"><form>
            <label class="songLine">
                <div class="leftLine">
                    <div class="songPlace">1</div>
                    <div class="songName">Artist One - Song One</div>
                </div>
            </label>
            <div class="naba-top-song"><div class="song_vote_info">
                <div class="place_previous">2</div>
            </div></div>
        </form></div>
        <div class="songsList"><form></form></div>
        </body></html>
        """
        soup = BeautifulSoup(html, 'lxml').css.select('.songsList')
        with pytest.raises(LookupError, match='No tag for date found'):
            parse_chart_data.fn(soup)

    def test_raises_when_song_not_in_db(self):
        """LookupError raised when song in HTML is not found in songs table"""
        html = """
        <html><body>
        <div class="songsList"><form>
            <label class="songLine">
                <div class="leftLine">
                    <div class="songPlace">1</div>
                    <div class="songName">Unknown Artist - Unknown Song</div>
                </div>
            </label>
            <div class="naba-top-song"><div class="song_vote_info">
                <div class="place_previous">2</div>
            </div></div>
            <div class="newsCard__date songListDate">13.02.2026</div>
        </form></div>
        <div class="songsList"><form>
            <div class="newsCard__date songListDate">13.02.2026</div>
        </form></div>
        </body></html>
        """
        soup = BeautifulSoup(html, 'lxml').css.select('.songsList')
        with pytest.raises(LookupError, match='not found in database'):
            parse_chart_data.fn(soup)

    def test_raises_when_no_songname_tag(self):
        """LookupError raised when songName tag is missing from songLine"""
        html = """
        <html><body>
        <div class="songsList"><form>
            <label class="songLine">
                <div class="leftLine">
                    <div class="songPlace">1</div>
                </div>
            </label>
            <div class="naba-top-song"><div class="song_vote_info">
                <div class="place_previous">2</div>
            </div></div>
            <div class="newsCard__date songListDate">13.02.2026</div>
        </form></div>
        <div class="songsList"><form>
            <div class="newsCard__date songListDate">13.02.2026</div>
        </form></div>
        </body></html>
        """
        soup = BeautifulSoup(html, 'lxml').css.select('.songsList')
        with pytest.raises(LookupError, match='No tag for web_songname found'):
            parse_chart_data.fn(soup)


class TestCreateChartsDf:
    def test_returns_polars_dataframe(self, charts_df):
        assert isinstance(charts_df, pl.DataFrame)

    def test_id_column_is_dropped(self, charts_df):
        assert 'id' not in charts_df.columns

    def test_correct_columns(self, charts_df):
        assert set(charts_df.columns) == {
            'song_id',
            'chart_type',
            'place',
            'week',
            'is_new_entry',
        }

    def test_correct_row_count(self, charts_df):
        assert len(charts_df) == 4


class TestInsertChartDataIntoDb:
    def test_inserts_chart_entries(self, charts_df):
        insert_chart_data_into_db.fn(charts_df)
        with duckdb.connect(config.DB_PATH) as conn:
            count = conn.sql('select count(*) from charts').fetchone()
        assert count is not None
        assert count[0] == 4

    def test_ignores_duplicates(self, charts_df):
        insert_chart_data_into_db.fn(charts_df)
        insert_chart_data_into_db.fn(charts_df)
        with duckdb.connect(config.DB_PATH) as conn:
            count = conn.sql('select count(*) from charts').fetchone()
        assert count is not None
        assert count[0] == 4

    def test_inserts_correct_chart_types(self, charts_df):
        insert_chart_data_into_db.fn(charts_df)
        with duckdb.connect(config.DB_PATH) as conn:
            top10_count = conn.sql(
                "select count(*) from charts where chart_type = 'top10'"
            ).fetchone()
            top25_count = conn.sql(
                "select count(*) from charts where chart_type = 'top25'"
            ).fetchone()
        assert top10_count is not None
        assert top10_count[0] == 2
        assert top25_count is not None
        assert top25_count[0] == 2

    def test_inserts_correct_week(self, charts_df):
        insert_chart_data_into_db.fn(charts_df)
        with duckdb.connect(config.DB_PATH) as conn:
            weeks = conn.sql('select distinct week from charts').fetchall()
        assert len(weeks) == 1
        assert weeks[0][0] == date(2026, 2, 13)
