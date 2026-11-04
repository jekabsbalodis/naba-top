MODEL (
  name sqlmesh_data.dim.songs,
  kind VIEW,
  grain song_id
);

SELECT
  song_id,
  artist,
  song_name
FROM sqlmesh_data.raw.songs
