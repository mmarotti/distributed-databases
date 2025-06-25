SELECT
    n.name AS neighborhood_name,
    SUM(c.total_armed_robbery_cellphone) AS total_armed_robbery_cellphone,
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
    neighborhood n ON v.neighborhood_id = n.id
WHERE
    n.name = 'SANTA EFIGÊNIA' AND t.year = 2015
GROUP BY
    n.name;