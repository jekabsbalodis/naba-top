"""
Type stub file for anodb.DB object.
The file is intended to provide static type checking for the dynamicaly created methods.
"""

import anodb


class DBConnection(anodb.DB):
    def create_songs_table(self) -> None:
        """Create the table where the songs will be stored"""
        ...

    def create_charts_table(self) -> None:
        """Create the table where the charts will be stored"""
        ...


db: DBConnection
