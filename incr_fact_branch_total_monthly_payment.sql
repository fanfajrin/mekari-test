MERGE INTO `db-mekari.talenta_dim_fact.fact_branch_total_monthly_payment` AS `target`
USING (
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
) AS `source`
ON `target`.`_id` = `source`.`_id`
WHEN MATCHED THEN
    UPDATE SET 
        target._id = source._id,
        target.branch_id = source.branch_id,
        target.month = source.month,
        target.year = source.year,
        target.duration = source.duration
WHEN NOT MATCHED THEN
    INSERT (
          _id
          ,branch_id
          ,month
          ,year
          ,duration
      )
    VALUES(
        source._id,
        source.branch_id,
        source.month,
        source.year,
        source.duration
    );