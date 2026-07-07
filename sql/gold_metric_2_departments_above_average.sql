-- Metric 2: departments that hired more employees in 2021 than the average
-- number of employees hired per department in 2021.
--
-- Assumption: the average is computed only over departments that had at
-- least one hire in 2021 (i.e. AVG over the per-department hire counts),
-- and departments with zero 2021 hires are excluded from both the average
-- and the output - consistent with "average number of employees hired by
-- department".
CREATE SCHEMA IF NOT EXISTS gold;

DROP TABLE IF EXISTS gold.departments_above_average_hiring_2021;

CREATE TABLE gold.departments_above_average_hiring_2021
USING DELTA
COMMENT 'Departments hiring above the 2021 per-department average (Metric 2)'
AS
WITH hires_2021 AS (
    SELECT department_id, COUNT(*) AS hired
    FROM silver.hired_employees
    WHERE year(hire_datetime) = 2021
    GROUP BY department_id
),
avg_hires AS (
    SELECT AVG(hired) AS avg_hired FROM hires_2021
)
SELECT
    d.id         AS id,
    d.department AS department,
    h.hired      AS hired
FROM hires_2021 h
JOIN silver.departments d ON d.id = h.department_id
CROSS JOIN avg_hires a
WHERE h.hired > a.avg_hired
ORDER BY hired DESC;