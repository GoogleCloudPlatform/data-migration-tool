CREATE TABLE edw_odb.departments
   ( department_id NUMBER(4)
   , department_name VARCHAR2(30)
   CONSTRAINT dept_name_nn NOT NULL
   , manager_id NUMBER(6)
   , location_id NUMBER(4)
   ) ;