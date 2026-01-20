-- name: create_songs_table#
-- Create the table where the songs will be stored
CREATE SEQUENCE IF NOT EXISTS song_id_sequence START 1;
CREATE TABLE IF NOT EXISTS songs (
    id UINT16 PRIMARY KEY DEFAULT nextval('song_id_sequence'),
    artist VARCHAR,
    song_name VARCHAR,
    web_songname VARCHAR,
    UNIQUE (artist, song_name)
);
CREATE INDEX IF NOT EXISTS idx_songs_artist ON songs (artist);

-- name: create_charts_table#
-- Create the table where the charts will be stored
CREATE SEQUENCE IF NOT EXISTS chart_id_sequence START 1;
CREATE TABLE IF NOT EXISTS charts (
    id UINT32 PRIMARY KEY DEFAULT nextval('chart_id_sequence'),
    song_id UINT16 NOT NULL,
    chart_type VARCHAR NOT NULL CHECK (chart_type IN ('top10', 'top25')),
    place UINT8,
    week DATE NOT NULL,
    is_new_entry BOOL DEFAULT false,
    FOREIGN KEY (song_id) REFERENCES songs (id),
    UNIQUE (song_id, week, chart_type)
);
CREATE INDEX IF NOT EXISTS idx_charts_week ON charts (week);
CREATE INDEX IF NOT EXISTS idx_charts_type_week ON charts (chart_type, week);
CREATE INDEX IF NOT EXISTS idx_charts_song_id ON charts (song_id);
