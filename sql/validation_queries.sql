-- Row counts across every layer. Run these after each pipeline execution.

SELECT 'bronze.departments_raw' AS table_name, COUNT(*) AS row_count FROM bronze.departments_raw
UNION ALL
SELECT 'bronze.jobs_raw', COUNT(*) FROM bronze.jobs_raw
UNION ALL
SELECT 'bronze.hired_employees_raw', COUNT(*) FROM bronze.hired_employees_raw
UNION ALL
SELECT 'silver.departments', COUNT(*) FROM silver.departments
UNION ALL
SELECT 'silver.jobs', COUNT(*) FROM silver.jobs
UNION ALL
SELECT 'silver.hired_employees', COUNT(*) FROM silver.hired_employees
UNION ALL
SELECT 'silver.rejected_records', COUNT(*) FROM silver.rejected_records
UNION ALL
SELECT 'gold.hires_by_department_job_quarter_2021', COUNT(*) FROM gold.hires_by_department_job_quarter_2021
UNION ALL
SELECT 'gold.departments_above_average_hiring_2021', COUNT(*) FROM gold.departments_above_average_hiring_2021;

-- Reconciliation: every bronze.hired_employees_raw row must end up in exactly
-- one bucket - either silver.hired_employees (valid) or
-- silver.rejected_records (format-invalid or superseded duplicate). The two
-- final counters must match, table_name identifies the entity.
SELECT
    (SELECT COUNT(*) FROM bronze.hired_employees_raw) AS bronze_raw_count,
    (SELECT COUNT(*) FROM silver.hired_employees) AS silver_valid_count,
    (SELECT COUNT(*) FROM silver.rejected_records WHERE table_name = 'hired_employees') AS rejected_count,
    (SELECT COUNT(*) FROM silver.hired_employees)
        + (SELECT COUNT(*) FROM silver.rejected_records WHERE table_name = 'hired_employees') AS reconciled_total;

-- Same reconciliation for departments and jobs.
SELECT
    'departments' AS entity,
    (SELECT COUNT(*) FROM bronze.departments_raw) AS bronze_raw_count,
    (SELECT COUNT(*) FROM silver.departments) AS silver_valid_count,
    (SELECT COUNT(*) FROM silver.rejected_records WHERE table_name = 'departments') AS rejected_count
UNION ALL
SELECT
    'jobs',
    (SELECT COUNT(*) FROM bronze.jobs_raw),
    (SELECT COUNT(*) FROM silver.jobs),
    (SELECT COUNT(*) FROM silver.rejected_records WHERE table_name = 'jobs');

-- Breakdown of rejection reasons, useful to eyeball what the data-quality
-- rules actually caught in a given run.
SELECT table_name, rejection_reason, COUNT(*) AS n
FROM silver.rejected_records
GROUP BY table_name, rejection_reason
ORDER BY table_name, rejection_reason;

-- Duplicate check: must return zero rows if the load strategy is working.
SELECT id, COUNT(*) AS n
FROM silver.hired_employees
GROUP BY id
HAVING COUNT(*) > 1;

-- Referential integrity check: must return zero rows (every FK in Silver
-- has already been validated, this simply re-confirms it).
SELECT h.id, h.department_id, h.job_id
FROM silver.hired_employees h
LEFT JOIN silver.departments d ON h.department_id = d.id
LEFT JOIN silver.jobs j ON h.job_id = j.id
WHERE d.id IS NULL OR j.id IS NULL;

-- Gold sanity check: total 2021 hires per department, collapsing Metric 1
-- across quarters and jobs - must match the per-department counts that feed
-- Metric 2's average.
SELECT department, SUM(Q1 + Q2 + Q3 + Q4) AS total_hires
FROM gold.hires_by_department_job_quarter_2021
GROUP BY department
ORDER BY total_hires DESC;
