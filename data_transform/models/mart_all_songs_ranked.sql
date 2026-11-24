MODEL (
  name sqlmesh_data.mart.all_songs_ranked,
  kind VIEW,
  grain (artist, song_name),
  audits (
    unique_combination_of_columns(columns := (artist, song_name)),
    not_null(columns := (place, score))
  )
);

WITH song_scores AS (
  SELECT
    c.artist,
    c.song_name,
    count(*) AS weeks_in_chart,
    fsum(
      CASE
        WHEN c.chart_type = 'top10' AND NOT c.place IS NULL
          THEN (
            11 - c.place
          )
        WHEN c.chart_type = 'top25'
          THEN (
            (
              26 - c.place
            ) / 2.5
          )
        ELSE 0
      END
    ) AS normalized_points,
    fsum(
      CASE
        WHEN c.chart_type = 'top10' AND NOT c.place IS NULL
          THEN (
            11 - c.place
          )
        WHEN c.chart_type = 'top25'
          THEN (
            26 - c.place
          )
        ELSE 0
      END
    ) AS raw_points,
    CASE
      WHEN bool_or(cast(c.chart_type = 'top10' AS BOOLEAN))
        THEN 'top10'
      ELSE 'top25'
    END AS chart_type
  FROM sqlmesh_data.fct.charts AS c
  WHERE
    NOT c.place IS NULL
  GROUP BY
    c.artist,
    c.song_name
)

SELECT
  rank()
    OVER (
      ORDER BY
        ss.normalized_points DESC, ss.weeks_in_chart ASC, ss.raw_points DESC
    )
    AS place,
  ss.artist,
  ss.song_name,
  ss.weeks_in_chart,
  ss.raw_points,
  ss.normalized_points AS score,
  ss.chart_type
FROM song_scores AS ss
ORDER BY
  place ASC
