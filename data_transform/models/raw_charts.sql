MODEL (
  name sqlmesh_data.raw.charts,
  kind FULL,
  grain chart_id,
  references song_id
);

SELECT
  id AS chart_id,
  song_id,
  chart_type,
  place,
  week,
  is_new_entry
FROM raw_data.main.charts
