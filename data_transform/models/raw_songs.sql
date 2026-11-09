MODEL (
  name sqlmesh_data.raw.songs,
  kind FULL,
  grain song_id,
  audits (
    unique_values(columns := (song_id, web_songname)),
    unique_combination_of_columns(columns := (artist, song_name)),
    not_null(columns := (
      song_id
    )),
    number_of_rows(threshold := 0)
  )
);

SELECT
  id AS song_id,
  artist,
  song_name,
  web_songname
FROM raw_data.main.songs
