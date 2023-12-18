CREATE TABLE edw_odb.jobs
   ( job_id VARCHAR2(10)
   , job_title VARCHAR2(35)
   CONSTRAINT job_title_nn NOT NULL
   , min_salary NUMBER(6)
   , max_salary NUMBER(6)
   ) ;