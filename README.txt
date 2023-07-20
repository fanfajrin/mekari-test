====mekari test=====

NAME: Isfan Fajrin

=====readme files====



Environment

1. Airflow
2. Gooogle Bucket
3. Bigquery

How to use my codes :)

1. dag_mekari_pipeline.py as a config of airflow pipeline, I utilize airflow to transport local file
to gcs bucket and bigquery
2. all filename that started with init_ is for initialization to create the table, you can customize
it by yourself for example add partition because i didnt add it yet.
3. all filename that started with incr_ is for incremental purpose, sadly the raw files doesnt have
created_at or updated_at column, so I cant slice it.
4. filename with dm on it is for datamart you can query (refer to test files that mekari sent) to that
specific table.

Enjoy

may the force be with you