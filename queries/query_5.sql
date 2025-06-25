SELECT
    SUM(c.total_armed_robbery_cellphone) AS total_armed_robbery_cellphone,
    SUM(c.total_armed_robbery_auto) AS total_armed_robbery_auto
FROM
    crime c
JOIN
    time t ON c.time_id = t.id
WHERE
    t.year = 2017;