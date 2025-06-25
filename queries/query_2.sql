SELECT
    s.id AS segment_id,
    s.geometry AS segment_geometry,
    SUM(c.total_feminicide) AS total_feminicide,
    SUM(c.total_homicide) AS total_homicide,
    SUM(c.total_felony_murder) AS total_felony_murder,
    SUM(c.total_bodily_harm) AS total_bodily_harm,
    SUM(c.total_theft_cellphone) AS total_theft_cellphone,
    SUM(c.total_armed_robbery_cellphone) AS total_armed_robbery_cellphone,
    SUM(c.total_theft_auto) AS total_theft_auto,
    SUM(c.total_armed_robbery_auto) AS total_armed_robbery_auto
FROM
    crime c
JOIN
    time t ON c.time_id = t.id
JOIN
    segment s ON c.segment_id = s.id
JOIN
    vertice v ON s.start_vertice_id = v.id -- start ou end?
JOIN
    district d ON v.district_id = d.id
WHERE
    d.name = 'IGUATEMI' AND t.year BETWEEN 2006 AND 2016
GROUP BY
    s.id, s.geometry
ORDER BY
    s.id;