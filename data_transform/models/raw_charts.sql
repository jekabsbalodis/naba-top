MODEL (
  name sqlmesh_data.raw.charts,
  kind FULL,
  grain chart_id,
  references song_id,
  audits (
    unique_values(columns := (
      chart_id
    )),
    not_null(columns := (chart_id, song_id, chart_type, week, is_new_entry)),
    accepted_values("column" := chart_type, is_in := ('top10', 'top25')),
    number_of_rows(threshold := 0)
  )
);

SELECT
  id AS chart_id,
  song_id,
  chart_type,
  place,
  week,
  is_new_entry
FROM raw_data.main.charts
