DROP TABLE IF EXISTS schools_by_locality;

CREATE TABLE schools_by_locality AS
(
    SELECT lc_ply_pid AS locality_id, vic_loca_2 AS locality_name, count(*) AS schools
    FROM locality l
    INNER JOIN school s
        ON ST_Intersects(l.boundary, s.location) = True
    GROUP BY lc_ply_pid, vic_loca_2;
);

INSERT INTO schools_by_locality (locality_id, locality_name, schools)
SELECT lc_ply_pid AS locality_id, vic_loca_2 AS locality_name, 0 AS schools
FROM locality l
WHERE NOT EXISTS
(
    SELECT 1 FROM schools_by_locality sbl WHERE l.locality_id = sbl.lc_ply_pid
);
