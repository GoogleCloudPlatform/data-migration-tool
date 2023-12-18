CREATE TABLE edw_odb.job_history
   ( employee_id NUMBER(6)
   CONSTRAINT jhist_employee_nn NOT NULL
   , start_date DATE
   CONSTRAINT jhist_start_date_nn NOT NULL
   , end_date DATE
   CONSTRAINT jhist_end_date_nn NOT NULL
   , job_id VARCHAR2(10)
   CONSTRAINT jhist_job_nn NOT NULL
   , department_id NUMBER(4)
   , CONSTRAINT jhist_date_interval
   CHECK (end_date > start_date)
   ) ;