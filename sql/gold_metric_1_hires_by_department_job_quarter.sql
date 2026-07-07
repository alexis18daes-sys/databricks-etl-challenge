-- Metric 1: number of employees hired per department and job in 2021, split by quarter.
CREATE SCHEMA IF NOT EXISTS gold;

DROP TABLE IF EXISTS gold.hires_by_department_job_quarter_2021;

CREATE TABLE gold.hires_by_department_job_quarter_2021
USING DELTA
COMMENT 'Hires per department/job/quarter for 2021 (Metric 1)'
AS
SELECT
    d.department AS department,
    j.job        AS job,
    SUM(CASE WHEN quarter(h.hire_datetime) = 1 THEN 1 ELSE 0 END) AS Q1,
    SUM(CASE WHEN quarter(h.hire_datetime) = 2 THEN 1 ELSE 0 END) AS Q2,
    SUM(CASE WHEN quarter(h.hire_datetime) = 3 THEN 1 ELSE 0 END) AS Q3,
    SUM(CASE WHEN quarter(h.hire_datetime) = 4 THEN 1 ELSE 0 END) AS Q4
FROM silver.hired_employees h
JOIN silver.departments d ON h.department_id = d.id
JOIN silver.jobs j        ON h.job_id = j.id
WHERE year(h.hire_datetime) = 2021
GROUP BY d.department, j.job
ORDER BY department ASC, job ASC;