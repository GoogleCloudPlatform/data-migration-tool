CREATE MULTISET TABLE DMT_DATASET.EMPLOYEE6(
  emp_no INTEGER,
  emp_name VARCHAR(50),
  job_title VARCHAR(30),
  manager_id INTEGER,
  hire_date Date,
  salary DECIMAL(18,2),
  commission DECIMAL(18,2),
  dept_no INTEGER
)
Primary Index(emp_no);
 
ALTER TABLE TUTORIAL_DB.EMPLOYEE ADD create_ts timestamp(0) default current_timestamp(0);
 
CREATE MULTISET TABLE TUTORIAL_DB.DEPARTMENT(
  dept_no INTEGER,
  department_name VARCHAR(30),
  loc_name  VARCHAR(30)
)
Primary Index(dept_no);

