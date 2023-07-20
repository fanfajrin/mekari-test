CREATE OR REPLACE TABLE `db-mekari.talenta_dim_fact.fact_branch_payroll`
AS
WITH BASE AS (
  SELECT
    branch_id
    ,SUM(dim.salary) AS monthly_salary
  FROM `db-mekari.talenta_dim_fact.dim_employees_detail`AS dim
  WHERE ACTIVE_FLAG = 'Y'
  GROUP BY 1
)
SELECT * FROM BASE