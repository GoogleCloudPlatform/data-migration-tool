-- Multi Statement

INSERT INTO DMT_DATASET.EMPLOYEE1(emp_no, emp_name, job_title, manager_id, salary, commission, dept_no) VALUES(2, 'emp1-test2', '2Engineer', 102, 2000000, 20000, 1002);
INSERT INTO DMT_DATASET.EMPLOYEE1(emp_no, emp_name, job_title, manager_id, salary, commission, dept_no) VALUES(3, 'emp1-test3', '3Engineer', 103, 3000000, 30000, 1003);
INSERT INTO DMT_DATASET.EMPLOYEE1(emp_no, emp_name, job_title, manager_id, salary, commission, dept_no) VALUES(4, 'emp1-test4', '4Engineer', 104, 4000000, 40000, 1004);
INSERT INTO DMT_DATASET.EMPLOYEE1(emp_no, emp_name, job_title, manager_id, salary, commission, dept_no) VALUES(5, 'emp1-test5', '5Engineer', 105, 5000000, 50000, 1005);
INSERT INTO DMT_DATASET.EMPLOYEE1(emp_no, emp_name, job_title, manager_id, salary, commission, dept_no) VALUES(6, 'emp1-test6', '6Engineer', 106, 6000000, 60000, 1006);
INSERT INTO DMT_DATASET.EMPLOYEE1(emp_no, emp_name, job_title, manager_id, salary, commission, dept_no) VALUES(7, 'emp1-test7', '7Engineer', 107, 7000000, 70000, 1007);

INSERT INTO DMT_DATASET.EMPLOYEE2(emp_no, emp_name, job_title, manager_id, salary, commission, dept_no) VALUES(101, 'emp2-test101', '101Engineer', 1011, 11000000, 210000, 10011);
INSERT INTO DMT_DATASET.EMPLOYEE2(emp_no, emp_name, job_title, manager_id, salary, commission, dept_no) VALUES(102, 'emp2-test102', '102Engineer', 1022, 12000000, 220000, 10022);
INSERT INTO DMT_DATASET.EMPLOYEE2(emp_no, emp_name, job_title, manager_id, salary, commission, dept_no) VALUES(103, 'emp2-test103', '103Engineer', 1033, 13000000, 230000, 10033);
INSERT INTO DMT_DATASET.EMPLOYEE2(emp_no, emp_name, job_title, manager_id, salary, commission, dept_no) VALUES(104, 'emp2-test104', '104Engineer', 1044, 14000000, 240000, 10044);
INSERT INTO DMT_DATASET.EMPLOYEE2(emp_no, emp_name, job_title, manager_id, salary, commission, dept_no) VALUES(105, 'emp2-test105', '105Engineer', 1055, 15000000, 250000, 10055);
INSERT INTO DMT_DATASET.EMPLOYEE2(emp_no, emp_name, job_title, manager_id, salary, commission, dept_no) VALUES(106, 'emp2-test106', '106Engineer', 1066, 16000000, 260000, 10066);
INSERT INTO DMT_DATASET.EMPLOYEE2(emp_no, emp_name, job_title, manager_id, salary, commission, dept_no) VALUES(107, 'emp2-test107', '107Engineer', 1077, 17000000, 270000, 10077);

UPDATE DMT_DATASET.EMPLOYEE1 SET emp_name = 'emp1-test1-changed' where emp_no = 1;
UPDATE DMT_DATASET.EMPLOYEE2 SET emp_name = 'emp2-test105-changed' where emp_no = 105;

DELETE FROM DMT_DATASET.EMPLOYEE1 where emp_no IN (2,3,4,5);
DELETE FROM DMT_DATASET.EMPLOYEE2 where emp_no = 1;