df_hive_tables_path = "temp_df/get_hive_tables_func/{hive_db_name}/hive_tables.csv"
df_partition_clustering_path = "temp_df/get_partition_clustering_info_func/{hive_db_name}/partition_clustering_info.csv"
df_text_format_schema_path = (
    "temp_df/get_text_format_schema_func/{hive_db_name}/text_format_schema.csv"
)
df_dvt_tables_list = "temp_df/get_dvt_table_list_func/{hive_db_name}/dvt_table_list.csv"
hive_tbl_gcs_path = "gs://{bkt_id}/{path}/{tbl}"
df_inc_tables_list = "temp_df/{dt}/get_inc_table_list/inc_table_list.csv"
df_inc_table_list_metadata = (
    "temp_df/{dt}/get_table_info_from_metadata/inc_table_list_metadata.csv"
)
df_partition_clustering_inc_tbl_path = (
    "temp_df/{dt}/get_partition_clustering_info_func/inc_partition_clustering_info.csv"
)
df_text_format_schema_inc_tbl_path = (
    "temp_df/{dt}/get_text_format_schema_func/inc_text_format_schema.csv"
)


query_rerun_n = (
    "select distinct table,format,case when (partition_flag = 'N'   and "
    " cluster_flag = 'N') "
    "then 'N' else 'Y' end as partition_flag, field_delimiter "
    "from {bq_dataset_audit}.{hive_ddl_metadata} where ddl_extracted='YES' and database = '{hive_db_name}' "
    "and start_time = (select max(start_time) from {bq_dataset_audit}.{hive_ddl_metadata}  where ddl_extracted='YES' and database = '{hive_db_name}' )"
)

query_rerun_y = (
    "select distinct hv.table,hv.format,hv.partition_flag,hv.field_delimiter from "
    "(select distinct table,format,  case when  (partition_flag = 'N'   and "
    " cluster_flag = 'N') then 'N' else 'Y' end as partition_flag,field_delimiter "
    " from {bq_dataset_audit}.{hive_ddl_metadata} where ddl_extracted='YES' and database='{hive_db_name}'  "
    "and start_time = (select max(start_time) from {bq_dataset_audit}.{hive_ddl_metadata} where ddl_extracted='YES' and database='{hive_db_name}' )) hv "
    "left join "
    "(select distinct tablename,load_dtm from {bq_dataset_audit}.{bq_load_audit} where hive_db_name='{hive_db_name}' and load_status='PASS' )bq "
    " on hv.table=bq.tablename "
    "where bq.tablename is null"
)

# TODO verify query logic and latest start timestamp
query_dvt_y = (
    "with fail_rec as (select distinct source_table_name  from {bq_dataset_audit}.{dvt_results} where validation_status='fail' ), "
    "pass_rec as (select distinct source_table_name from {bq_dataset_audit}.{dvt_results} where validation_status='success' ) "
    "select distinct split(pass_rec.source_table_name ,'.')[OFFSET(1)] as table from pass_rec left join fail_rec on fail_rec.source_table_name=pass_rec.source_table_name "
    "where fail_rec.source_table_name  is null "
)

# TODO check for sql_file_name column in the table, should be table name?
query_dvt_n = (
    "select distinct split(sql_file_name,'.')[OFFSET(0)] as table from {bq_dataset_audit}.{schema_results_tbl} where execution_start_time = "
    "(select max(execution_start_time) from {bq_dataset_audit}.{schema_results_tbl})"
)

query_dvt_n_backup = (
    "select distinct split(sql_file_name,'.')[OFFSET(0)] as table from {bq_dataset_audit}.{schema_results_tbl} where status='success' and execution_start_time = "
    "(select max(execution_start_time) from {bq_dataset_audit}.{schema_results_tbl})"
)


query_partition_clustering_info = (
    "select table_name,STRING_AGG(partition_column) as partition_column ,STRING_AGG(clustering_column) as clustering_column from "
    "((SELECT table_name,column_name as partition_column, null as clustering_column  FROM {bq_dataset_name}.INFORMATION_SCHEMA.COLUMNS "
    " where is_partitioning_column = 'YES' and table_name in ({table_names})) "
    "union all  "
    "(SELECT table_name, null as partition_column,  STRING_AGG(column_name ORDER BY clustering_ordinal_position ) clustering_column "
    " FROM {bq_dataset_name}.INFORMATION_SCHEMA.COLUMNS "
    "where clustering_ordinal_position is not null and table_name in ({table_names}) "
    "GROUP BY table_name))"
    "group by table_name"
)

query_text_format_schema = (
    "select table_name, STRING_AGG(schema_string) as schema_string from "
    "( select table_name, (column_name || ':' || (split(data_type,'(')[SAFE_OFFSET(0)])) as schema_string "
    "FROM {bq_dataset_name}.INFORMATION_SCHEMA.COLUMNS "
    "where is_partitioning_column = 'NO' and table_name in ({table_names}) "
    "order by ordinal_position ) "
    "group by table_name"
)

dvt_query_table_list = (
    "select distinct concat('{hive_db_name}.',tablename,' = {bq_dataset_name}.',tablename) as tablename FROM "
    "{bq_dataset_audit}.{bq_load_audit} where load_status = 'PASS' "
    "and load_dtm = (SELECT max(load_dtm) from {bq_dataset_audit}.{bq_load_audit})"
)

# Queries for Incremental Load
# Get data from pubsub audit table

hive_pubsub_audit_files = (
    "SELECT distinct json_extract_scalar(TO_JSON_STRING(data),'$.name') as name "
    "FROM {bq_dataset_audit}.{bq_pubsub_audit_tbl} "
    "where publish_time between "
    "datetime_sub(CAST('{schedule_runtime}' AS TIMESTAMP), INTERVAL 1 day) and "
    "CAST('{schedule_runtime}' AS TIMESTAMP)"
)

query_get_incremental_list_of_tables = (
    "SELECT distinct bq_dataset, table_name, "
    "table_name as concat_db_tbl, destination_path "
    "FROM {bq_dataset_audit}.{bq_inc_copy_status_tbl} "
    "where date(job_run_time) = ( "
    "SELECT max(date(job_run_time)) "
    "FROM {bq_dataset_audit}.{bq_inc_copy_status_tbl}) "
    "and upper(file_copy_status) = 'PASS' "
)

query_get_inc_table_metadata = (
    "select distinct bq_dataset, table,format,case when (partition_flag = 'N'   and "
    " cluster_flag = 'N') "
    "then 'N' else 'Y' end as partition_flag, field_delimiter ,concat(bq_dataset, '.',table) as concat_db_tbl, database "
    "from {bq_dataset_audit}.{hive_ddl_metadata} where ddl_extracted='YES' and concat(bq_dataset, '.',table) in ({tbl_list}) "
)

query_get_bq_dataset = "select distinct bq_dataset from {bq_dataset_audit}.{hive_ddl_metadata} where table='{table}' and database='{hive_db}'"

query_inc_tbl_partition_clustering_info = (
    "select bq_dataset_name,table_name,STRING_AGG(partition_column) as partition_column ,STRING_AGG(clustering_column) as clustering_column, concat_db_tbl from "
    "((SELECT '{bq_dataset_name}' as bq_dataset_name, table_name,column_name as partition_column, null as clustering_column, concat('{bq_dataset_name}','.',table_name) as concat_db_tbl "
    "FROM {bq_dataset_name}.INFORMATION_SCHEMA.COLUMNS "
    " where is_partitioning_column = 'YES' and table_name in ({table_names})) "
    "union all  "
    "(SELECT '{bq_dataset_name}' as bq_dataset_name,table_name, null as partition_column,  STRING_AGG(column_name ORDER BY clustering_ordinal_position ) clustering_column, "
    "concat('{bq_dataset_name}','.',table_name) as concat_db_tbl "
    " FROM {bq_dataset_name}.INFORMATION_SCHEMA.COLUMNS "
    "where clustering_ordinal_position is not null and table_name in ({table_names}) "
    "GROUP BY bq_dataset_name,table_name, concat_db_tbl))"
    "group by bq_dataset_name,table_name, concat_db_tbl"
)


query_inc_tbl_text_format_schema_info = (
    "select bq_dataset_name,table_name, concat_db_tbl, STRING_AGG(schema_string) as schema_string from "
    "( select '{bq_dataset_name}' as bq_dataset_name, table_name,concat('{bq_dataset_name}','.',table_name) as concat_db_tbl, "
    "(column_name || ':' || (split(data_type,'(')[SAFE_OFFSET(0)])) as schema_string "
    "FROM {bq_dataset_name}.INFORMATION_SCHEMA.COLUMNS "
    "where is_partitioning_column = 'NO' and table_name in ({table_names}) "
    "order by ordinal_position ) "
    "group by bq_dataset_name,table_name, concat_db_tbl "
)
