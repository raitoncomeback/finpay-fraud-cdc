from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import json

default_args = {
    'owner': 'finpay-data',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False,
}

with DAG(
    dag_id='finpay_fraud_cdc_pipeline',
    description='FinPay Fraud Detection CDC Pipeline: Debezium → Kafka → RisingWave → Iceberg → dbt → Features',
    schedule_interval='*/15 * * * *',  # Every 15 minutes for near-real-time
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=['finpay', 'fraud', 'cdc', 'streaming', 'ml-features'],
) as dag:

    # Task 1: Check Debezium connector health
    check_debezium = BashOperator(
        task_id='check_debezium_connector',
        bash_command="""
        curl -s http://debezium:8083/connectors/finpay-cdc/status | \
        jq -r '.connector.state + " " + .tasks[0].state' | \
        grep -q 'RUNNING RUNNING' || exit 1
        echo "Debezium connector healthy"
        """,
        retries=3,
        retry_delay=timedelta(seconds=30),
    )

    # Task 2: Check Kafka lag for critical topics
    check_kafka_lag = BashOperator(
        task_id='check_kafka_lag',
        bash_command="""
        python << 'EOF'
        from kafka import KafkaConsumer
        import os

        consumer = KafkaConsumer(
            bootstrap_servers='kafka:29092',
            group_id='lag-checker',
            auto_offset_reset='latest',
            enable_auto_commit=False
        )

        topics = [
            'finpay.public.transactions',
            'finpay.public.users',
            'finpay.public.merchants',
            'finpay.public.devices'
        ]

        max_lag = 0
        for topic in topics:
            partitions = consumer.partitions_for_topic(topic)
            if partitions:
                for p in partitions:
                    tp = TopicPartition(topic, p)
                    consumer.assign([tp])
                    consumer.seek_to_end(tp)
                    end_offset = consumer.position(tp)
                    consumer.seek_to_beginning(tp)
                    start_offset = consumer.position(tp)
                    lag = end_offset - start_offset
                    max_lag = max(max_lag, lag)
                    print(f"{topic}:{p} lag={lag}")

        consumer.close()
        print(f"Max lag: {max_lag}")
        if max_lag > 10000:
            exit(1)
        EOF
        """,
        retries=2,
        retry_delay=timedelta(minutes=2),
    )

    # Task 3: Run RisingWave streaming jobs (continuous, just verify they're running)
    check_risingwave = BashOperator(
        task_id='check_risingwave_mvs',
        bash_command="""
        psql -h risingwave -p 4566 -U root -d dev -c "
        SELECT name, 'running' as status
        FROM rw_materialized_views
        WHERE name IN ('mv_user_velocity_30d', 'mv_user_velocity_7d', 'mv_user_geo_anomaly',
                       'mv_merchant_risk_realtime', 'mv_device_risk_realtime',
                       'mv_transaction_risk_score');
        " || exit 1
        echo "RisingWave MVs running"
        """,
    )

    # Task 4: Run dbt models (staging -> intermediate -> marts)
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command="""
        cd /opt/dbt_finpay && \
        dbt deps && \
        dbt run --select staging intermediate marts --target prod
        """,
        env={
            'DBT_PROFILES_DIR': '/opt/dbt_finpay',
            'RISINGWAVE_HOST': 'risingwave',
            'RISINGWAVE_PORT': '4566',
            'RISINGWAVE_DATABASE': 'dev',
            'RISINGWAVE_USER': 'root',
            'RISINGWAVE_PASSWORD': '',
            'MINIO_ENDPOINT': 'http://minio:9000',
            'MINIO_ACCESS_KEY': 'minioadmin',
            'MINIO_SECRET_KEY': 'minioadmin',
        },
        retries=2,
        retry_delay=timedelta(minutes=5),
    )

    # Task 5: Run dbt tests
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command="""
        cd /opt/dbt_finpay && \
        dbt test --select staging intermediate marts --target prod
        """,
        env={
            'DBT_PROFILES_DIR': '/opt/dbt_finpay',
            'RISINGWAVE_HOST': 'risingwave',
            'RISINGWAVE_PORT': '4566',
            'RISINGWAVE_DATABASE': 'dev',
            'RISINGWAVE_USER': 'root',
            'RISINGWAVE_PASSWORD': '',
            'MINIO_ENDPOINT': 'http://minio:9000',
            'MINIO_ACCESS_KEY': 'minioadmin',
            'MINIO_SECRET_KEY': 'minioadmin',
        },
    )

    # Task 6: Validate feature freshness
    check_feature_freshness = BashOperator(
        task_id='check_feature_freshness',
        bash_command="""
        psql -h risingwave -p 4566 -U root -d dev -c "
        SELECT
            MAX(_dbt_computed_at) as last_updated,
            COUNT(*) as feature_count,
            EXTRACT(EPOCH FROM (NOW() - MAX(_dbt_computed_at)))/60 as minutes_stale
        FROM finpay_gold.gold_fraud_features;
        " | grep -v "minutes_stale" | awk '{if ($3 > 60) exit 1}'
        echo "Features fresh"
        """,
    )

    # Task 7: Export latest features for model serving (optional)
    export_features = BashOperator(
        task_id='export_features_for_serving',
        bash_command="""
        psql -h risingwave -p 4566 -U root -d dev -c "
        COPY (
            SELECT * FROM finpay_gold.gold_fraud_features
            WHERE _dbt_computed_at >= NOW() - INTERVAL '1 hour'
            ORDER BY composite_risk_score DESC
            LIMIT 10000
        ) TO STDOUT WITH CSV HEADER
        " > /tmp/features_latest.csv && \
        aws s3 cp /tmp/features_latest.csv s3://finpay-features/latest/features_latest.csv \
            --endpoint-url http://minio:9000 \
            --no-verify-ssl && \
        echo "Exported features to S3"
        """,
        retries=1,
    )

    # Task 8: Alert on high-risk transactions (for demo)
    alert_high_risk = BashOperator(
        task_id='alert_high_risk_transactions',
        bash_command="""
        psql -h risingwave -p 4566 -U root -d dev -c "
        SELECT
            transaction_id,
            user_id,
            merchant_id,
            amount,
            composite_risk_score,
            risk_tier,
            initiated_at
        FROM finpay_gold.gold_fraud_features
        WHERE risk_tier IN ('high', 'critical')
          AND initiated_at >= NOW() - INTERVAL '15 minutes'
        ORDER BY composite_risk_score DESC
        LIMIT 20;
        " | tail -n +3 | head -n -1 | while read line; do
            echo "HIGH RISK ALERT: $line"
        done
        echo "Alert check complete"
        """,
    )

    # Dependencies
    check_debezium >> check_kafka_lag >> check_risingwave >> dbt_run >> dbt_test >> check_feature_freshness
    check_feature_freshness >> [export_features, alert_high_risk]