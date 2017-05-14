DROP VIEW IF EXISTS vw_locality;
CREATE VIEW vw_locality AS
(
    SELECT gid, lc_ply_pid, dt_create, dt_retire,
           loc_pid, vic_locali, vic_loca_1, vic_loca_2,
           vic_loca_3, vic_loca_4, vic_loca_5, vic_loca_6, 
           vic_loca_7, ST_AsGeoJSON(boundary) as boundary, 
           schools_count
    FROM locality
);
