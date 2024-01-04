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

CREATE TABLE `project_id`.DMT_DATASET.EMPLOYEE6_TARGET
(
    emp_no INTEGER,
    emp_name VARCHAR(50),
    job_title VARCHAR(30),
    manager_id INTEGER,
    hire_date Date,
    salary DECIMAL(18,2),
    commission DECIMAL(18,2),
    dept_no INTEGER
);
