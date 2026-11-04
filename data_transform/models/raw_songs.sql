MODEL (
  name sqlmesh_data.raw.songs,
  kind FULL,
  grain song_id
);

SELECT
  id AS song_id,
  artist,
  song_name,
  web_songname
FROM raw_data.main.songs
