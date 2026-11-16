from datetime import date

import duckdb
import polars as pl
import pytest
from bs4 import BeautifulSoup

from models import ChartType
from pipeline.update_charts import (
    create_charts_df,
    extract_chart_elements,
    insert_chart_data_into_db,
    parse_chart_data,
    validate_charts_count,
)
from tests.helpers import chart_html, page_html, song_line

SONGS = [
    ('Artist One', 'Song One', 'Artist One - Song One'),
    ('Artist Two', 'Song Two', 'Artist Two - Song Two'),
    ('Artist Three', 'Song Three', 'Artist Three - Song Three'),
    ('Artist Four', 'Song Four', 'Artist Four - Song Four'),
]

# Top10: One is ranked, Two is unranked ('j'). Top25: the same for Three and Four.
PAGE = page_html(
    chart_html(
        [song_line('Artist One - Song One', 1), song_line('Artist Two - Song Two', 'j')]
    ),
    chart_html(
        [
            song_line('Artist Three - Song Three', 1),
            song_line('Artist Four - Song Four', 'j'),
        ]
    ),
)


def elements(html: str):
    return extract_chart_elements(BeautifulSoup(html, 'lxml'))


@pytest.fixture(autouse=True)
def songs_in_db(db_path):
    """Charts refer to songs by id, so the songs must exist first."""
    with duckdb.connect(db_path) as conn:
        conn.executemany(
            'insert into songs (artist, song_name, web_songname) values (?, ?, ?)',
            SONGS,
        )


@pytest.fixture
def entries(db_path):
    return parse_chart_data(elements(PAGE), db_path)


def count_charts(db_path: str) -> int:
    with duckdb.connect(db_path) as conn:
        row = conn.execute('select count(*) from charts').fetchone()
    assert row is not None
    return row[0]


# --- extract_chart_elements -------------------------------------------------


def test_extract_finds_both_charts():
    assert len(elements(PAGE)) == 2


def test_extract_from_empty_page_finds_nothing():
    assert len(elements('<html></html>')) == 0


# --- parse_chart_data -------------------------------------------------------


def test_parse_returns_an_entry_per_song_with_its_chart_type(entries):
    assert [e.chart_type for e in entries] == [
        ChartType.TOP10,
        ChartType.TOP10,
        ChartType.TOP25,
        ChartType.TOP25,
    ]


def test_parse_reads_places_and_treats_j_as_unranked(entries):
    assert [e.place for e in entries] == [1, None, 1, None]


def test_parse_reads_the_week_day_first(entries):
    assert {e.week for e in entries} == {date(2026, 2, 13)}


def test_parse_links_entries_to_song_ids(entries):
    assert [e.song_id for e in entries] == [1, 2, 3, 4]


def test_everything_is_new_when_charts_are_empty(entries):
    assert all(e.is_new_entry for e in entries)


def test_ranked_songs_already_in_charts_are_not_new(db_path, entries):
    insert_chart_data_into_db(create_charts_df(entries), db_path)

    again = parse_chart_data(elements(PAGE), db_path)

    ranked = [e for e in again if e.place is not None]
    unranked = [e for e in again if e.place is None]
    assert ranked
    assert not any(e.is_new_entry for e in ranked)
    assert all(e.is_new_entry for e in unranked)  # unranked always counts as new


def test_parse_fails_without_a_date(db_path):
    html = page_html(
        chart_html([song_line('Artist One - Song One', 1)], date=None), chart_html([])
    )
    with pytest.raises(LookupError, match='date'):
        parse_chart_data(elements(html), db_path)


def test_parse_fails_for_a_song_missing_from_the_db(db_path):
    html = page_html(
        chart_html([song_line('Unknown Artist - Unknown Song', 1)]), chart_html([])
    )
    with pytest.raises(LookupError, match='not found in database'):
        parse_chart_data(elements(html), db_path)


def test_parse_fails_for_a_song_row_without_a_name(db_path):
    nameless = '<label class="songLine"><div class="songPlace">1</div></label>'
    html = page_html(chart_html([nameless]), chart_html([]))
    with pytest.raises(LookupError, match='web_songname'):
        parse_chart_data(elements(html), db_path)


# --- validate_charts_count --------------------------------------------------


def charts_df(top10_ranked=10, top25_ranked=25, unranked=5) -> pl.DataFrame:
    rows = []
    for chart_type, ranked in (('top10', top10_ranked), ('top25', top25_ranked)):
        rows += [(chart_type, place) for place in range(1, ranked + 1)]
        rows += [(chart_type, None)] * unranked
    return pl.DataFrame(rows, schema=['chart_type', 'place'], orient='row')


def test_expected_counts_pass():
    validate_charts_count(charts_df())


@pytest.mark.parametrize(
    ('counts', 'message'),
    [
        ({'top10_ranked': 9}, 'top10 has 9 ranked'),
        ({'top25_ranked': 24}, 'top25 has 24 ranked'),
        ({'top10_ranked': 0}, 'top10 has 0 ranked'),
    ],
)
def test_wrong_counts_fail_and_unranked_songs_do_not_make_up_for_it(counts, message):
    with pytest.raises(ValueError, match=message):
        validate_charts_count(charts_df(**counts))


# --- create_charts_df / insert_chart_data_into_db ---------------------------


def test_charts_df_has_no_id_column(entries):
    df = create_charts_df(entries)
    assert set(df.columns) == {'song_id', 'chart_type', 'place', 'week', 'is_new_entry'}
    assert len(df) == 4


def test_insert_adds_the_entries(db_path, entries):
    insert_chart_data_into_db(create_charts_df(entries), db_path)
    with duckdb.connect(db_path) as conn:
        rows = conn.execute(
            'select chart_type, count(*) from charts group by 1 order by 1'
        )
        assert rows.fetchall() == [('top10', 2), ('top25', 2)]


def test_insert_ignores_entries_that_exist(db_path, entries):
    df = create_charts_df(entries)
    insert_chart_data_into_db(df, db_path)
    insert_chart_data_into_db(df, db_path)
    assert count_charts(db_path) == 4
