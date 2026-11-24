MODEL (
  name sqlmesh_data.mart.top100,
  kind VIEW,
  grain (artist, song_name),
  audits (
    unique_combination_of_columns(columns := (artist, song_name)),
    unique_values(columns := (
      final_place
    )),
    not_null(columns := (final_place, artist, song_name))
  )
);

WITH deduplicated AS (
  SELECT
    place,
    artist,
    song_name,
    weeks_in_chart,
    raw_points,
    score,
    chart_type
  FROM (
    SELECT
      place,
      artist,
      song_name,
      weeks_in_chart,
      raw_points,
      score,
      chart_type,
      row_number() OVER (PARTITION BY artist ORDER BY place) AS artist_rank
    FROM sqlmesh_data.mart.all_songs_ranked
  )
  WHERE
    artist_rank = 1
), top10_part AS (
  SELECT
    place,
    artist,
    song_name,
    weeks_in_chart,
    raw_points,
    score,
    chart_type
  FROM (
    SELECT
      place,
      artist,
      song_name,
      weeks_in_chart,
      raw_points,
      score,
      chart_type,
      row_number() OVER (ORDER BY place ASC) AS quota_rank
    FROM deduplicated
    WHERE
      chart_type = 'top10'
  )
  WHERE
    quota_rank <= @top100_top10_quota
), remaining_songs AS (
  SELECT
    d.place,
    d.artist,
    d.song_name,
    d.weeks_in_chart,
    d.raw_points,
    d.score,
    d.chart_type,
    row_number() OVER (ORDER BY d.place ASC) AS fill_rank
  FROM deduplicated AS d
  WHERE
    NOT EXISTS(
      SELECT
        1
      FROM top10_part AS t
      WHERE
        t.artist = d.artist AND t.song_name = d.song_name
    )
), combined AS (
  SELECT
    place,
    artist,
    song_name,
    weeks_in_chart,
    raw_points,
    score,
    chart_type
  FROM top10_part
  UNION ALL
  SELECT
    place,
    artist,
    song_name,
    weeks_in_chart,
    raw_points,
    score,
    chart_type
  FROM remaining_songs
  WHERE
    fill_rank <= @top100_total - (
      SELECT
        count(*)
      FROM top10_part
    )
)
SELECT
  rank() OVER (ORDER BY place ASC, score DESC, raw_points DESC, weeks_in_chart ASC) AS final_place,
  place,
  artist,
  song_name,
  weeks_in_chart,
  raw_points,
  score,
  chart_type
FROM combined
ORDER BY
  final_place ASC
