from spark.utils import write_df_to_postgres, setup_logger

logger = setup_logger("load", "/opt/airflow/logs/load.log")

def load_transformed_to_postgres(df, jdbc_url: str, user: str, password: str, table_name: str):
    write_df_to_postgres(df, table_name, jdbc_url, user, password, mode="overwrite")
    logger.info(f"Transformed data loaded into PostgreSQL table: {table_name}")

def load_kpis_to_postgres(kpi_dict: dict, jdbc_url: str, user: str, password: str):
    for name, df in kpi_dict.items():
        write_df_to_postgres(df, name, jdbc_url, user, password, mode="overwrite")
        logger.info(f"KPI '{name}' loaded into PostgreSQL")