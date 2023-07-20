from airflow import DAG 
from airflow.models import Variable
from airflow.contrib.operators.mysql_to_gcs import MySqlToGoogleCloudStorageOperator 
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.dummy_operator import DummyOperator 
from airflow.operators.python import BranchPythonOperator


credentials = service_account.Credentials.from_service_account_file('/opt/airflow/dags/mp/config/mekari_talenta_cred.json')


dag = DAG ( 
	'dag_mekari_pipeline', 
	tags                    = ['pipeline'], 
	default_args            = default_args, 
	catchup                 = False, 
	schedule_interval       = '0 1 * * *', #scheduler here boiiii
	template_searchpath     = TEMPLATE_SEARCHPATH,
	start_date              = datetime(2020, 4, 5)
	)


push_employees = LocalFilesystemToGCSOperator(
    task_id="push",
    src='/opt/airflow/dags/mekari/ext_file/employees.csv',
    dst='mekari/employees.csv',
    gcp_conn_id='mekari_gcp',
    bucket=bucket,
    )

push_timesheets = LocalFilesystemToGCSOperator(
    task_id="push",
    src='/opt/airflow/dags/mekari/ext_file/timesheets.csv',
    dst='mekari/timesheets.csv',
    gcp_conn_id='mekari_gcp',
    bucket=bucket,
    )


#load to bq
load_employees = GoogleCloudStorageToBigQueryOperator ( 
	task_id                             = 'load', 
	bucket                              = bucket, 
	destination_project_dataset_table   = 'db-mekari.raw_db.employees',
	source_objects                      = ['mekari/employees.csv'],
	field_delimiter                     = ',',
	skip_leading_rows                   = 1, 
	allow_quoted_newlines               = True, 
	create_disposition                  = 'CREATE_IF_NEEDED', 
	write_disposition                   = 'WRITE_TRUNCATE', 
	ignore_unknown_values               = True, 
	autodetect                          = True,
	dag                                 = dag
	) 

load_timesheets = GoogleCloudStorageToBigQueryOperator ( 
	task_id                             = 'load', 
	bucket                              = bucket, 
	destination_project_dataset_table   = 'db-mekari.raw_db.timesheets',
	source_objects                      = ['mekari/timesheets.csv'],
	field_delimiter                     = ',',
	skip_leading_rows                   = 1, 
	allow_quoted_newlines               = True, 
	create_disposition                  = 'CREATE_IF_NEEDED', 
	write_disposition                   = 'WRITE_TRUNCATE', 
	ignore_unknown_values               = True, 
	autodetect                          = True,
	dag                                 = dag
	) 

start_task >> push_employees >> push_timesheets >> load_employees >> load_timesheets >> end_task 