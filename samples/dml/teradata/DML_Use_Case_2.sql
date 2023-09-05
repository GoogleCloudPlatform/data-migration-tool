-- Error due to invalid_col not exist
UPDATE DMT_DATASET.EMPLOYEE1 SET emp_name = 'emp1-test1-changed', invalid_col = 'emp1-test1-address' where emp_no = 101;