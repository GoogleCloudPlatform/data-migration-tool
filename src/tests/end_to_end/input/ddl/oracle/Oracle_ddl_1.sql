CREATE TABLE edw_odb.regions
   ( region_id NUMBER 
   CONSTRAINT region_id_nn NOT NULL 
   , region_name VARCHAR2(25) 
   );