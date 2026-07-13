"""
FinPay CDC Monitoring DAG
Monitors CDC lag, data freshness, and schema changes
Runs every 5 minutes
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'finpay',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'email_on_failure': False,
}

with DAG(
    dag_id='finpay_cdc_monitor',
    description='Monitor CDC pipeline health: lag, throughput, schema changes',
    schedule_interval='*/5 * * * *',  # Every 5 minutes
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['finpay', 'cdc', 'monitoring'],
    default_args=default_args,
) as dag:

    def check_cdc_lag():
        """Check Kafka consumer lag for CDC topics"""
        from kafka import KafkaConsumer, TopicPartition
        from loguru import logger
        
        topics = [
            'finpay.public.transactions',
            'finpay.public.users',
            'finpay.public.merchants',
            'finpay.public.devices',
        ]
        
        consumer = KafkaConsumer(
            bootstrap_servers=['kafka:29092'],
            group_id='cdc-lag-monitor',
            enable_auto_commit=False
        )
        
        lag_report = {}
        for topic in topics:
            partitions = consumer.partitions_for_topic(topic)
            if partitions:
                tp_list = [TopicPartition(topic, p) for p in partitions]
                consumer.assign(tp_list)
                consumer.poll(timeout_ms=1000)
                
                end_offsets = consumer.end_offsets(tp_list)
                committed = consumer.committed(tp_list)
                
                total_lag = 0
                for tp in tp_list:
                    end_offset = end_offsets.get(tp, 0)
                    committed_offset = committed.get(tp, 0) or 0
                    lag = end_offset - committed_offset
                    total_lag += lag
                
                lag_report[topic] = {
                    'lag': total_lag,
                    'partitions': len(partitions)
                }
                
                # Alert if lag > threshold
                if total_lag > 10000:
                    logger.warning(f"HIGH LAG ALERT: {topic} has lag of {total_lag}")
        
        consumer.close()
        logger.info(f"CDC Lag Report: {lag_report}")
        return lag_report

    def check_data_freshness():
        """Check data freshness in RisingWave"""
        import psycopg2
        from loguru import logger
        
        conn = psycopg2.connect(
            host='risingwave',
            port=4566,
            database='dev',
            user='root',
            password=''
        )
        
        freshness_checks = {}
        
        with conn.cursor() as cur:
            # Check bronze transactions freshness
            cur.execute("""
                SELECT 
                    MAX(_cdc_ts) as latest_cdc_ts,
                    MAX(_ingested_at) as latest_ingested_at,
                    EXTRACT(EPOCH FROM (NOW() - MAX(_cdc_ts))) / 60 as minutes_since_cdc
                FROM bronze_transactions
            """)
            row = cur.fetchone()
            if row:
                freshness_checks['bronze_transactions'] = {
                    'latest_cdc_ts': str(row[0]),
                    'latest_ingested_at': str(row[1]),
                    'minutes_since_cdc': float(row[2]) if row[2] else None
                }
                if row[2] and row[2] > 10:
                    logger.warning(f"STALE DATA: bronze_transactions {row[2]} min behind")
            
            # Check silver transactions freshness
            cur.execute("""
                SELECT 
                    MAX(_cdc_ts) as latest_cdc_ts,
                    EXTRACT(EPOCH FROM (NOW() - MAX(_cdc_ts))) / 60 as minutes_since_cdc
                FROM silver_transactions_enriched
            """)
            row = cur.fetchone()
            if row:
                freshness_checks['silver_transactions'] = {
                    'latest_cdc_ts': str(row[0]),
                    'minutes_since_cdc': float(row[1]) if row[1] else None
                }
            
            # Check gold fraud features freshness
            cur.execute("""
                SELECT 
                    MAX(_dbt_computed_at) as latest_dbt_ts,
                    EXTRACT(EPOCH FROM (NOW() - MAX(_dbt_computed_at))) / 60 as minutes_since_dbt
                FROM gold_fraud_features
            """)
            row = cur.fetchone()
            if row:
                freshness_checks['gold_fraud_features'] = {
                    'latest_dbt_ts': str(row[0]),
                    'minutes_since_dbt': float(row[1]) if row[1] else None
                }
            
            # Check record counts
            cur.execute("""
                SELECT 
                    (SELECT COUNT(*) FROM bronze_transactions) as bronze_txns,
                    (SELECT COUNT(*) FROM silver_transactions_enriched) as silver_txns,
                    (SELECT COUNT(*) FROM gold_fraud_features) as gold_features,
                    (SELECT COUNT(DISTINCT user_id) FROM gold_user_fraud_features) as active_users
            """)
            row = cur.fetchone()
            freshness_checks['counts'] = {
                'bronze_transactions': row[0],
                'silver_transactions': row[1],
                'gold_features': row[2],
                'active_users': row[3]
            }
        
        conn.close()
        logger.info(f"Data Freshness: {freshness_checks}")
        return freshness_checks

    def check_schema_changes():
        """Check for schema changes in source tables"""
        import psycopg2
        from loguru import logger
        
        # Connect to source Postgres
        conn = psycopg2.connect(
            host='finpay-postgres',
            port=5432,
            database='finpay',
            user='finpay',
            password='finpay123'
        )
        
        with conn.cursor() as cur:
            cur.execute("""
                SELECT table_name, column_name, data_type, column_default, is_nullable
                FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name IN ('transactions', 'users', 'merchants', 'devices')
                ORDER BY table_name, ordinal_position
            """)
            columns = cur.fetchall()
        
        conn.close()
        
        # In production, compare with stored schema and alert on changes
        logger.info(f"Schema check: {len(columns)} columns found")
        return {"columns_checked": len(columns)}
    
    def check_feature_api_health():
        """Check fraud feature API health"""
        import requests
        from loguru import logger
        
        try:
            response = requests.get('http://fraud-api:8000/health', timeout=5)
            if response.status_code == 200:
                health = response.json()
                logger.info(f"Feature API Health: {health}")
                return health
            else:
                logger.error(f"Feature API unhealthy: {response.status_code}")
                return {"status": "unhealthy", "code": response.status_code}
        except Exception as e:
            logger.error(f"Feature API check failed: {e}")
            return {"status": "error", "error": str(e)}

    t1 = PythonOperator(
        task_id='check_cdc_lag',
        python_callable=check_cdc_lag,
    )

    t2 = PythonOperator(
        task_id='check_data_freshness',
        python_callable=check_data_freshness,
    )

    t3 = PythonOperator(
        task_id='check_schema_changes',
        python_callable=check_schema_changes,
    )

    t4 = PythonOperator(
        task_id='check_feature_api_health',
        python_callable=check_feature_api_health,
    )

    # All monitoring tasks run in parallel
    [t1, t2, t3, t4]