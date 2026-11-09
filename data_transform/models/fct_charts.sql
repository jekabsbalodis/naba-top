MODEL (
  name sqlmesh_data.fct.charts,
  kind VIEW,
  grain chart_id,
  audits (
    unique_values(columns := (
      chart_id
    )),
    unique_combination_of_columns(columns := (week, artist, song_name, chart_type, place))
  )
);

SELECT
  c.chart_id,
  c.week,
  s.artist,
  s.song_name,
  c.chart_type,
  c.place,
  c.is_new_entry
FROM sqlmesh_data.raw.charts AS c
JOIN sqlmesh_data.dim.songs AS s
  ON c.song_id = s.song_id
