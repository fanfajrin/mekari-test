CREATE OR REPLACE TABLE `db-mekari.talenta_dim_fact.dim_employees_detail`
AS
WITH BASE AS(
  SELECT 
    employe_id
    ,branch_id
    ,salary
    ,join_date
    ,resign_date
    ,CASE WHEN resign_date IS NULL THEN 'Y'
      ELSE 'N'
      END AS ACTIVE_FLAG
    ,CASE WHEN resign_date IS NULL THEN DATE_DIFF(CURRENT_DATE(), join_date, DAY)
    ELSE  DATE_DIFF(resign_date, join_date, DAY) END AS employe_life
  FROM `db-mekari.raw_db.employees`
)
SELECT * FROM BASE