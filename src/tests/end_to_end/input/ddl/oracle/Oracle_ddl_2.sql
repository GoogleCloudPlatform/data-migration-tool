CREATE TABLE edw_odb.countries 
   ( country_id CHAR(2) 
   CONSTRAINT country_id_nn NOT NULL 
   , country_name VARCHAR2(40) 
   , region_id NUMBER 
   , CONSTRAINT country_c_id_pk 
   PRIMARY KEY (country_id) 
   ) 
   ORGANIZATION INDEX; 