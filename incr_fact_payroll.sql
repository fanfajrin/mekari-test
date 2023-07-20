MERGE INTO `db-mekari.talenta_dim_fact.fact_branch_payroll` AS `target`
USING (
WITH BASE AS (
  SELECT
    branch_id
    ,SUM(dim.salary) AS monthly_salary
  FROM `db-mekari.talenta_dim_fact.dim_employees_detail` AS dim
  WHERE ACTIVE_FLAG = 'Y'
  GROUP BY 1
)
SELECT * FROM BASE
) AS `source`
ON `target`.`branch_id` = `source`.`branch_id`
WHEN MATCHED THEN
    UPDATE SET 
        
        target.branch_id = source.branch_id,
        target.monthly_salary = source.monthly_salary

WHEN NOT MATCHED THEN
    INSERT (
          branch_id
          ,monthly_salary

      )
    VALUES(
        source.branch_id,
        source.monthly_salary

    );
