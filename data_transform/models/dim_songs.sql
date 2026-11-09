MODEL (
  name sqlmesh_data.dim.songs,
  kind VIEW,
  grain song_id,
  audits (
    unique_values(columns := (
      song_id
    )),
    unique_combination_of_columns(columns := (artist, song_name))
  )
);

SELECT
  song_id,
  artist,
  song_name
FROM sqlmesh_data.raw.songs
