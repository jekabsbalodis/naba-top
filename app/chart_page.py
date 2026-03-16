"""Chart page module for displaying music charts in the Streamlit application."""

import polars as pl
import streamlit as st

from app.data.get_data import get_chart, get_date_range
from app.state.manage_state import StateKeys, load_state_value
from app.utils.format import get_date_string
from app.widgets.widgets import shared_slider
from models import ChartType


def _create_page(
    chart_type: ChartType, title: str, description_chart_type: str
) -> None:
    """Create a chart page and specify variables for it.

    Args:
        chart_type: The type of chart for which to create the page.
        title: Text to display as a title of the page.
        description_chart_type: Text to display in description paragraph
        (the type of chart).

    """
    load_state_value(key=StateKeys.SELECTED_WEEK)
    _, latest_week = get_date_range()

    if chart_type == ChartType.TOP10:
        chart_df, _ = get_chart(week=st.session_state[StateKeys.SELECTED_WEEK])
    elif chart_type == ChartType.TOP25:
        _, chart_df = get_chart(week=st.session_state[StateKeys.SELECTED_WEEK])
    else:
        raise LookupError('Nav izvēlēts tops, par kuru attēlot datus.')

    new_entries = chart_df.filter(pl.col('is_new_entry'))['artist', 'song_name']

    column_config = {
        'artist': st.column_config.TextColumn(
            label='Izpildītājs',
        ),
        'song_name': st.column_config.TextColumn(
            label='Dziesmas nosakums',
        ),
        'place': st.column_config.NumberColumn(
            label='Vieta',
            width=50,
            pinned=True,
        ),
    }

    ###
    # Page contents
    ###

    if title is None:
        raise ValueError('Nav norādīts lapas virsraksts.')
    else:
        st.title(title)

    col1, col2 = st.columns([3, 7], gap='medium')

    with col1:
        if description_chart_type is None:
            raise ValueError('Nav norādīts lapas apraksts')
        else:
            st.markdown(f"""
                        Šī sadaļa paredzēta {description_chart_type} sarakstu apskatei.
                        Izvēlies datumu, un aplūko, kāds bija attiecīgās nedēļas tops.
                        """)

        shared_slider()
        st.divider()
        st.text(
            f'{get_date_string(st.session_state[StateKeys.SELECTED_WEEK])}'
            ' - šīs nedēļas jaunumi topā.',
            help=(
                'Jaunumi ir tās dziesmas, kuras iepriekš topā nav bijušas'
                ' vai kurām nav piešķirta vieta.'
            ),
        )

        st.dataframe(
            new_entries,
            height=35 * len(new_entries) + 38,
            hide_index=True,
            placeholder='-',
            column_config=column_config,
            column_order=['artist', 'song_name'],
        )

    with col2:
        st.dataframe(
            chart_df,
            height=35 * len(chart_df) + 38,
            hide_index=True,
            placeholder='-',
            column_config=column_config,
            column_order=['place', 'artist', 'song_name'],
        )


def top10_page() -> None:
    """Create page for Latvian music Top 10 chart page."""
    _create_page(
        chart_type=ChartType.TOP10,
        title='Latvijas mūzikas Top&nbsp;10',
        description_chart_type='Top&nbsp;10',
    )


def top25_page() -> None:
    """Create page for foreign music Top  25 chart page."""
    _create_page(
        chart_type=ChartType.TOP25,
        title='Ārzemju mūzikas Top&nbsp;25',
        description_chart_type='Top&nbsp;25',
    )
