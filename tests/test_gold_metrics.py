from datetime import datetime

from src.common.tables import ensure_schema
from src.gold.build_gold import run_gold


def test_gold_metrics(spark):
    ensure_schema(spark, "silver")

    departments = spark.createDataFrame([(1, "Engineering"), (2, "Sales")], ["id", "department"])
    jobs = spark.createDataFrame([(1, "Engineer"), (2, "Recruiter")], ["id", "job"])
    hired = spark.createDataFrame(
        [
            (1, "A", datetime(2021, 1, 15), 1, 1),
            (2, "B", datetime(2021, 4, 15), 1, 1),
            (3, "C", datetime(2021, 4, 20), 1, 1),
            (4, "D", datetime(2021, 7, 1), 2, 2),
        ],
        ["id", "name", "hire_datetime", "department_id", "job_id"],
    )

    departments.write.format("delta").mode("overwrite").saveAsTable("silver.departments")
    jobs.write.format("delta").mode("overwrite").saveAsTable("silver.jobs")
    hired.write.format("delta").mode("overwrite").saveAsTable("silver.hired_employees")

    run_gold(spark)

    metric1 = {
        row["department"]: (row["Q1"], row["Q2"], row["Q3"], row["Q4"])
        for row in spark.sql("SELECT * FROM gold.hires_by_department_job_quarter_2021").collect()
    }
    # 1 hire in Q1, 2 in Q2, 0 in Q3/Q4 for Engineering/Engineer.
    assert metric1["Engineering"] == (1, 2, 0, 0)

    metric2 = spark.sql("SELECT * FROM gold.departments_above_average_hiring_2021").collect()
    # per-department hires: Engineering=3, Sales=1 -> average=2 -> only
    # Engineering (3 > 2) is above average.
    assert [r["department"] for r in metric2] == ["Engineering"]
    assert metric2[0]["hired"] == 3
