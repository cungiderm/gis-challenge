DROP VIEW IF EXISTS vw_school;
CREATE VIEW vw_school AS
(
    SELECT education_sector, entity_type, school_no, school_name,
           school_type, school_status, address_line_1, address_line_2, 
           address_town, address_state, address_postcode, postal_address_line_1, 
           postal_address_line_2, postal_town, postal_state, postal_postcode, 
           full_phone_no, lga_id, lga_name, x, y, ST_AsGeoJSON(location) AS location
    FROM school
);
