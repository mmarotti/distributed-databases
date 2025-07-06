-- Primeira opção

-- SELECT
--     SUM(c.total_feminicide) AS total_feminicide,
--     SUM(c.total_homicide) AS total_homicide,
--     SUM(c.total_felony_murder) AS total_felony_murder,
--     SUM(c.total_bodily_harm) AS total_bodily_harm,
--     SUM(c.total_theft_cellphone) AS total_theft_cellphone,
--     SUM(c.total_armed_robbery_cellphone) AS total_armed_robbery_cellphone,
--     SUM(c.total_theft_auto) AS total_theft_auto,
--     SUM(c.total_armed_robbery_auto) AS total_armed_robbery_auto
-- FROM
--     crime c
-- JOIN
--     time t ON c.time_id = t.id
-- JOIN
--     segment s ON c.segment_id = s.id
-- WHERE
--     s.oneway = TRUE AND t.year = 2012

-- Segunda opção

WITH exploded_crimes AS (
  SELECT
    c.segment_id,
    c.time_id,
    crime_type,
    crime_count
  FROM crime c
  LATERAL VIEW explode(
    map(
      'feminicide', c.total_feminicide,
      'homicide', c.total_homicide,
      'felony_murder', c.total_felony_murder,
      'bodily_harm', c.total_bodily_harm,
      'theft_cellphone', c.total_theft_cellphone,
      'armed_robbery_cellphone', c.total_armed_robbery_cellphone,
      'theft_auto', c.total_theft_auto,
      'armed_robbery_auto', c.total_armed_robbery_auto
    )
  ) exploded_table AS crime_type, crime_count
)

SELECT
  ec.crime_type,
  SUM(ec.crime_count) AS total_crimes
FROM exploded_crimes ec
JOIN segment s ON ec.segment_id = s.id
JOIN `time` t ON ec.time_id = t.id
WHERE
    s.oneway = 'yes'
    AND t.year = 2012
GROUP BY
  ec.crime_type