SELECT
    s.id AS segment_id,
    SUM(
        c.total_feminicide +
        c.total_homicide +
        c.total_felony_murder +
        c.total_bodily_harm +
        c.total_theft_cellphone +
        c.total_armed_robbery_cellphone +
        c.total_theft_auto +
        c.total_armed_robbery_auto
    ) AS total_crime_incidents
FROM
    crime c
JOIN
    time t ON c.time_id = t.id
JOIN
    segment s ON c.segment_id = s.id
WHERE
    t.month = 11 AND t.year = 2010
GROUP BY
    s.id
ORDER BY
    total_crime_incidents DESC;