CREATE OR REPLACE TABLE `db-mekari.talenta_dm.dm_branch_salary_per_hour`
AS
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