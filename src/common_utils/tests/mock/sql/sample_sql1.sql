CREATE MULTISET TABLE DMT_DATASET.EMPLOYEE1(
  emp_no INTEGER,
  emp_name VARCHAR(50),
  job_title VARCHAR(30),
  manager_id INTEGER,
  hire_date Date,
  salary DECIMAL(18,2),
  commission DECIMAL(18,2),
  dept_no INTEGER
);
CREATE TABLE `project_id`.DMT_DATASET.EMPLOYEE1_TARGET
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