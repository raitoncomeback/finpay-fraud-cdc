"""
FinPay dbt Daily Run DAG
Runs dbt models daily to refresh gold layer features
Also handles backfills and data quality checks
"""
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'finpay',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
}

with DAG(
    dag_id='finpay_dbt_daily',
    description='Daily dbt run for FinPay fraud features',
    schedule_interval='0 2 * * *',  # 2 AM daily
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['finpay', 'dbt', 'fraud', 'daily'],
    default_args=default_args,
) as dag:

    def check_gold_layer_health():
        """Validate gold layer before/after dbt run"""
        import psycopg2
        from loguru import logger
        
        conn = psycopg2.connect(
            host='risingwave',
            port=4566,
            database='dev',
            user='root',
            password=''
        )
        
        with conn.cursor() as cur:
            # Check row counts
            tables = [
                'gold_fraud_features',
                'gold_user_fraud_features', 
                'gold_merchant_risk_score',
                'gold_device_risk_score'
            ]
            
            for table in tables:
                cur.execute(f"SELECT COUNT(*) FROM {table}")
                count = cur.fetchone()[0]
                logger.info(f"{table}: {count} rows")
                
                # Validate non-empty for critical tables
                if table == 'gold_fraud_features' and count == 0:
                    raise Exception(f"CRITICAL: {table} is empty!")
                
                if table == 'gold_user_fraud_features' and count < 100:
                    logger.warning(f"WARNING: {table} has only {count} users")
        
        conn.close()
        return "health_check_passed"

    def backfill_if_needed():
        """Check if backfill is needed for late-arriving data"""
        import psycopg2
        from loguru import logger
        
        conn = psycopg2.connect(
            host='risingwave',
            port=4566,
            database='dev',
            user='root',
            password=''
        )
        
        with conn.cursor() as cur:
            # Check for transactions in last 2 days not in gold features
            cur.execute("""
                SELECT COUNT(*) 
                FROM bronze_transactions b
                LEFT JOIN gold_fraud_features g ON b.transaction_id = g.transaction_id
                WHERE b.initiated_at >= NOW() - INTERVAL '2 days'
                  AND g.transaction_id IS NULL
            """)
            missing = cur.fetchone()[0]
            
            if missing > 0:
                logger.warning(f"Found {missing} transactions missing from gold layer - backfill may be needed")
        
        conn.close()
        return {"missing_transactions": missing}

    t1 = PythonOperator(
        task_id='pre_run_health_check',
        python_callable=check_gold_layer_health,
    )

    t2 = BashOperator(
        task_id='dbt_deps',
        bash_command='cd /opt/airflow/dbt_finpay && dbt deps',
    )

    t3 = BashOperator(
        task_id='dbt_run_daily',
        bash_command='cd /opt/airflow/dbt_finpay && dbt run --profiles-dir . --target prod',
    )

    t4 = BashOperator(
        task_id='dbt_test_daily',
        bash_command='cd /opt/airflow/dbt_finpay && dbt test --profiles-dir . --target prod',
    )

    t5 = PythonOperator(
        task_id='post_run_health_check',
        python_callable=check_gold_layer_health,
    )

    t6 = PythonOperator(
        task_id='check_backfill_needed',
        python_callable=backfill_if_needed,
    )

    t1 >> t2 >> t3 >> t4 >> t5 >> t6