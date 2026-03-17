"""Data models for the music charts application.

This module defines the core data structures used throughout the application,
including chart types, song information, chart entries, and configuration models.
"""

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from enum import StrEnum, auto


class ChartType(StrEnum):
    """Enumeration of supported chart types.

    Attributes:
        TOP10: The top 10 songs chart.
        TOP25: The top 25 songs chart.

    """

    TOP10 = auto()
    TOP25 = auto()


@dataclass
class Song:
    """Represents a song with artist information.

    Attributes:
        artist: Name of the artist/band.
        song_name: Title of the song.
        web_songname: Combined artist-song string as displayed on the website.
        id: Database ID (None if not yet persisted).

    """

    artist: str
    song_name: str
    web_songname: str
    id: int | None = None


@dataclass
class ChartEntry:
    """Represents a song's position in a specific chart for a given week.

    Attributes:
        song_id: Foreign key to the Songs table.
        chart_type: Type of chart (TOP10 or TOP25).
        place: Position in the chart (1-10 or 1-25, None for unranked).
        week: Date of the chart week.
        is_new_entry: Whether this is the song's first appearance in charts,
        or if the song is in an unranked place.
        id: Database ID (None if not yet persisted).

    """

    song_id: int
    chart_type: ChartType
    place: int | None
    week: date
    is_new_entry: bool
    id: int | None = None


@dataclass
class TopEntry:
    """Represents a song's entry in the top charts for display purposes.

    Attributes:
        week: Date of the chart week.
        artist: Name of the artist/band.
        song_name: Title of the song.
        place: Position in the chart.
        is_new_entry: Whether this is the song's first appearance.
        or if the song is in an unranked place.

    """

    week: date
    artist: str
    song_name: str
    place: int | None
    is_new_entry: bool


@dataclass
class RankedSongEntry:
    """Represents a song with its chart performance metrics.

    Attributes:
        place: Current position in the chart.
        song_id: Foreign key to the Songs table.
        artist: Name of the artist/band.
        song_name: Title of the song.
        weeks_in_chart: Number of weeks the song has been in charts.
        raw_points: Points accumulated from chart positions.
        score: Calculated score (normalized points).
        chart_type: Type of chart (TOP10 or TOP25).

    """

    place: int
    song_id: int
    artist: str
    song_name: str
    weeks_in_chart: int
    raw_points: int
    score: Decimal
    chart_type: str


@dataclass
class S3Config:
    """Configuration for S3-compatible storage connections.

    Attributes:
        key_id: Access key ID for authentication.
        secret: Secret access key for authentication.
        endpoint: S3 endpoint URL.
        region: AWS region identifier.

    """

    key_id: str
    secret: str
    endpoint: str
    region: str
