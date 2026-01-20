from dataclasses import dataclass
from datetime import date
from enum import StrEnum, auto


class ChartType(StrEnum):
    TOP10 = auto()
    TOP25 = auto()


@dataclass
class Song:
    id: int | None
    artist: str
    song_name: str
    web_songname: str
    test: str


@dataclass
class ChartEntry:
    id: int | None
    song_id: int
    chart_type: ChartType
    place: int | None
    week: date
    is_new_entry: bool
    artist: str
    song_name: str
