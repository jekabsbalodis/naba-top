from dataclasses import dataclass
from datetime import date
from enum import StrEnum, auto


class ChartType(StrEnum):
    TOP10 = auto()
    TOP25 = auto()


@dataclass
class Song:
    artist: str
    song_name: str
    web_songname: str
    id: int | None = None


@dataclass
class ChartEntry:
    song_id: int
    chart_type: ChartType
    place: int | None
    week: date
    is_new_entry: bool
    id: int | None = None
