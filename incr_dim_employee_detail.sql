MERGE INTO `db-mekari.talenta_dim_fact.dim_employees_detail` AS `target`
USING (
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
  FROM db-mekari.raw_db.employees
)
SELECT * FROM BASE
) AS `source`
ON `target`.`employe_id` = `source`.`employe_id`
WHEN MATCHED THEN
    UPDATE SET 
        target.employe_id = source.employe_id,
        target.branch_id = source.branch_id,
        target.salary = source.salary,
        target.join_date = source.join_date,
        target.resign_date = source.resign_date,
        target.active_flag = source.active_flag,
        target.employe_life = source.employe_life
WHEN NOT MATCHED THEN
    INSERT (
      employe_id
      ,branch_id
      ,salary
      ,join_date
      ,resign_date
      ,active_flag
      ,employe_life
      )
    VALUES(
        source.employe_id,
        source.branch_id,
        source.salary,
        source.join_date,
        source.resign_date,
        source.active_flag,
        source.employe_life

    );
