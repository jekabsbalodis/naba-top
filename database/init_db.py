from database.connect_db import db


def main():
    db.create_songs_table()
    db.create_charts_table()


if __name__ == '__main__':
    main()
