"""
FinPay CDC Bootstrap DAG
Runs initial snapshot load from Postgres to Iceberg via RisingWave
Run once manually after infrastructure is up
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'finpay',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='finpay_cdc_bootstrap',
    description='One-time bootstrap: register Debezium connector, create RisingWave sources/tables, run initial dbt',
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['finpay', 'cdc', 'bootstrap'],
    default_args=default_args,
) as dag:

    def register_debezium():
        """Register Debezium connector for FinPay Postgres"""
        import requests
        import json
        import time
        
        connect_url = "http://debezium:8083/connectors"
        headers = {"Content-Type": "application/json"}
        
        config = {
            "name": "finpay-cdc",
            "config": {
                "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
                "database.hostname": "finpay-postgres",
                "database.port": "5432",
                "database.user": "finpay",
                "database.password": "finpay123",
                "database.dbname": "finpay",
                "database.server.name": "finpay",
                "table.include.list": "public.users,public.accounts,public.merchants,public.merchant_locations,public.devices,public.transactions,public.fraud_signals",
                "publication.name": "finpay_cdc",
                "slot.name": "finpay_cdc_slot",
                "plugin.name": "pgoutput",
                "transforms": "unwrap,addSource",
                "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
                "transforms.unwrap.drop.tombstones": "true",
                "transforms.unwrap.delete.handling.mode": "rewrite",
                "transforms.addSource.type": "org.apache.kafka.connect.transforms.InsertField$Value",
                "transforms.addSource.static.field": "cdc_source",
                "transforms.addSource.static.value": "finpay-postgres",
                "key.converter": "org.apache.kafka.connect.json.JsonConverter",
                "key.converter.schemas.enable": "false",
                "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                "value.converter.schemas.enable": "false",
                "snapshot.mode": "initial",
                "snapshot.locking.mode": "none",
                "decimal.handling.mode": "string",
                "time.precision.mode": "connect",
                "max.batch.size": "2048",
                "max.queue.size": "8192",
                "poll.interval.ms": "100",
                "include.schema.changes": "false"
            }
        }
        
        # Check if connector exists
        response = requests.get(f"{connect_url}/finpay-cdc")
        if response.status_code == 200:
            print("Connector already exists, updating...")
            requests.delete(f"{connect_url}/finpay-cdc")
            time.sleep(2)
        
        response = requests.post(connect_url, headers=headers, json=config)
        response.raise_for_status()
        print("Debezium connector registered successfully")
        return "success"

    def wait_for_cdc_topics():
        """Wait for CDC topics to be created and have data"""
        from kafka import KafkaConsumer
        import time
        
        topics = [
            'finpay.public.transactions',
            'finpay.public.users', 
            'finpay.public.merchants',
            'finpay.public.devices',
            'finpay.public.accounts',
            'finpay.public.merchant_locations'
        ]
        
        consumer = KafkaConsumer(
            bootstrap_servers=['kafka:29092'],
            group_id='cdc-bootstrap-check',
            auto_offset_reset='earliest',
            consumer_timeout_ms=5000
        )
        
        # Wait for topics to exist
        for topic in topics:
            for attempt in range(30):
                partitions = consumer.partitions_for_topic(topic)
                if partitions:
                    print(f"Topic {topic} exists with partitions: {partitions}")
                    break
                print(f"Waiting for topic {topic}... (attempt {attempt+1}/30)")
                time.sleep(10)
            else:
                raise Exception(f"Topic {topic} not found after 5 minutes")
        
        consumer.close()
        print("All CDC topics are available")
        return "success"

    def run_risingwave_ddl():
        """Execute RisingWave DDL to create sources and tables"""
        import psycopg2
        import time
        
        # Wait for RisingWave to be ready
        for attempt in range(30):
            try:
                conn = psycopg2.connect(
                    host='risingwave',
                    port=4566,
                    database='dev',
                    user='root',
                    password=''
                )
                conn.close()
                print("RisingWave is ready")
                break
            except Exception:
                print(f"Waiting for RisingWave... (attempt {attempt+1}/30)")
                time.sleep(10)
        else:
            raise Exception("RisingWave not ready after 5 minutes")
        
        # Execute DDL files in order
        ddl_files = [
            '/opt/airflow/risingwave/ddl_bronze.sql',
            '/opt/airflow/risingwave/ddl_silver.sql', 
            '/opt/airflow/risingwave/ddl_gold.sql'
        ]
        
        for ddl_file in ddl_files:
            print(f"Executing {ddl_file}")
            conn = psycopg2.connect(
                host='risingwave',
                port=4566,
                database='dev',
                user='root',
                password=''
            )
            with open(ddl_file, 'r') as f:
                sql = f.read()
            
            with conn.cursor() as cur:
                # Split by semicolon and execute each statement
                statements = [s.strip() for s in sql.split(';') if s.strip()]
                for stmt in statements:
                    if stmt:
                        try:
                            cur.execute(stmt)
                        except Exception as e:
                            print(f"Statement failed (may be expected): {e}")
                            print(f"Statement: {stmt[:200]}...")
            conn.commit()
            conn.close()
            print(f"Completed {ddl_file}")
        
        return "success"

    def run_dbt_models():
        """Run dbt models to build gold layer"""
        import subprocess
        import os
        
        os.chdir('/opt/airflow/dbt_finpay')
        
        # Run dbt deps
        result = subprocess.run(['dbt', 'deps'], capture_output=True, text=True)
        if result.returncode != 0:
            print(f"dbt deps failed: {result.stderr}")
            raise Exception("dbt deps failed")
        
        # Run dbt models
        result = subprocess.run(['dbt', 'run', '--profiles-dir', '.'], capture_output=True, text=True)
        print(result.stdout)
        if result.returncode != 0:
            print(f"dbt run failed: {result.stderr}")
            raise Exception("dbt run failed")
        
        # Run dbt tests
        result = subprocess.run(['dbt', 'test', '--profiles-dir', '.'], capture_output=True, text=True)
        print(result.stdout)
        if result.returncode != 0:
            print(f"dbt test failed: {result.stderr}")
            raise Exception("dbt test failed")
        
        return "success"

    # Task definitions
    t1 = PythonOperator(
        task_id='register_debezium',
        python_callable=register_debezium,
    )

    t2 = PythonOperator(
        task_id='wait_for_cdc_topics',
        python_callable=wait_for_cdc_topics,
    )

    t3 = PythonOperator(
        task_id='run_risingwave_ddl',
        python_callable=run_risingwave_ddl,
    )

    t4 = PythonOperator(
        task_id='run_dbt_models',
        python_callable=run_dbt_models,
    )

    t1 >> t2 >> t3 >> t4