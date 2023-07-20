MERGE INTO `db-mekari.talenta_dm.dm_branch_salary_per_hour` AS `target`
USING (
WITH BASE AS (
  SELECT
    fbranch.branch_id
    ,fbranch.month
    ,fbranch.year
    ,ROUND(SUM(fpay.monthly_salary)/SUM(fbranch.duration), 2)  as salary_per_hour
  FROM `db-mekari.talenta_dim_fact.fact_branch_total_monthly_payment` AS fbranch
  INNER JOIN `db-mekari.talenta_dim_fact.fact_branch_payroll`AS fpay
  ON fbranch.branch_id = fpay.branch_id
  GROUP BY 1,2,3
)
SELECT * FROM BASE
) AS `source`
ON `target`.`branch_id` = `source`.`branch_id`
WHEN MATCHED THEN
    UPDATE SET         
        target.branch_id = source.branch_id,
        target.month = source.month,
        target.year = source.year,
        target.salary_per_hour = source.salary_per_hour
WHEN NOT MATCHED THEN
    INSERT (
          branch_id
          ,month
          ,year
          ,salary_per_hour
      )
    VALUES(
        source.branch_id,
        source.month,
        source.year.
        source,salary_per_hour

    );