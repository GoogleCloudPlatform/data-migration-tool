CREATE TABLE edw_odb.locations
   ( location_id NUMBER(4)
   , street_address VARCHAR2(40)
   , postal_code VARCHAR2(12)
   , city VARCHAR2(30)
   CONSTRAINT loc_city_nn NOT NULL
   , state_province VARCHAR2(25)
   , country_id CHAR(2)
   ) ;