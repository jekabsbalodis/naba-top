MODEL (
  name sqlmesh_data.mart.top10,
  kind VIEW,
  grain (week, artist, song_name),
  audits (
    unique_combination_of_columns(columns := (week, artist, song_name)),
    not_null(columns := (week, artist, song_name))
  )
);

SELECT
  c.week,
  c.artist,
  c.song_name,
  c.place,
  c.is_new_entry
FROM sqlmesh_data.fct.charts AS c
WHERE
  c.chart_type = 'top10'
ORDER BY
  c.week DESC,
  c.place ASC
