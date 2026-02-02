from spark.utils import setup_logger, write_df_to_postgres

logger = setup_logger("load", "/opt/airflow/logs/load.log")

def load_to_postgres(df, jdbc_url: str, user: str, password: str, table_name: str):
    """
    Load data to PostgreSQL using OVERWRITE mode for idempotency.
    Each run replaces the table completely.
    """
    row_count = df.count()
    logger.info(f"Loading {row_count} rows to PostgreSQL table: {table_name}")
    
    write_df_to_postgres(df, table_name, jdbc_url, user, password, mode="overwrite")
    
    logger.info(f"Data loaded successfully to {table_name}")
    
    
    #Load the kpis

def load_kpis_to_postgres(kpi_dict: dict, jdbc_url: str, user: str, password: str):
    """Load all KPI tables to PostgreSQL"""
    for name, df in kpi_dict.items():
        load_to_postgres(df, jdbc_url, user, password, name)
        
        

def load_single_kpi(df, kpi_name: str, jdbc_url: str, user: str, password: str):
    """
    Load a single KPI table to PostgreSQL.
    Used when each KPI has its own task.
    """
    logger.info(f"Loading KPI: {kpi_name}")
    load_to_postgres(df, jdbc_url, user, password, kpi_name)