CREATE OR REPLACE TABLE `db-mekari.talenta_dim_fact.fact_branch_total_monthly_payment`
AS
WITH BASE AS(
  SELECT
    CONCAT(emp.branch_id,'-',FORMAT_DATE('%B', ts.date),'-',EXTRACT(YEAR FROM ts.date)) AS _id
    ,emp.branch_id
    ,FORMAT_DATE('%B', ts.date) AS month
    ,EXTRACT(YEAR FROM ts.date) AS year
    ,SUM(TIME_DIFF(ts.checkout, ts.checkin, HOUR)) AS duration
  FROM db-mekari.raw_db.employees AS emp
  INNER JOIN db-mekari.raw_db.timesheets AS ts
  ON emp.employe_id = ts.employee_id
  WHERE ts.checkout IS NOT NULL AND ts.checkin IS NOT NULL
  GROUP BY 1,2,3,4
  ORDER BY 3,2,1
  
)
SELECT * FROM BASE
WHERE duration IS NOT NULL