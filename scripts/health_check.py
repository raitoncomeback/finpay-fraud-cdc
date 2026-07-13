#!/usr/bin/env python3
"""
Health check script for FinPay Fraud CDC Pipeline
Run via cron or as a container health check
"""
import sys
import json
import time
import psycopg2
from kafka import KafkaConsumer, TopicPartition
import requests
from datetime import datetime, timedelta

# Configuration
KAFKA_BOOTSTRAP = "localhost:9092"
RISINGWAVE_HOST = "localhost"
RISINGWAVE_PORT = 4566
RISINGWAVE_DB = "dev"
RISINGWAVE_USER = "root"
DEBEZIUM_URL = "http://localhost:8083"
API_URL = "http://localhost:8000"

# Thresholds
MAX_CDC_LAG = 10000
MAX_FEATURE_STALE_MIN = 10
MAX_CDC_STALE_MIN = 5

def check_debezium():
    """Check Debezium connector status"""
    try:
        resp = requests.get(f"{DEBEZIUM_URL}/connectors/finpay-cdc/status", timeout=5)
        if resp.status_code != 200:
            return False, f"Connector not found: {resp.status_code}"
        
        status = resp.json()
        connector_state = status.get('connector', {}).get('state')
        task_states = [t.get('state') for t in status.get('tasks', [])]
        
        if connector_state != 'RUNNING':
            return False, f"Connector state: {connector_state}"
        if any(s != 'RUNNING' for s in task_states):
            return False, f"Task states: {task_states}"
        
        return True, "Debezium RUNNING"
    except Exception as e:
        return False, f"Debezium check failed: {e}"

def check_kafka_lag():
    """Check Kafka consumer lag for CDC topics"""
    try:
        consumer = KafkaConsumer(
            bootstrap_servers=[KAFKA_BOOTSTRAP],
            group_id='health-check',
            auto_offset_reset='latest',
            enable_auto_commit=False
        )
        
        topics = [
            'finpay.public.transactions',
            'finpay.public.users',
            'finpay.public.merchants',
            'finpay.public.devices',
            'finpay.public.accounts',
            'finpay.public.merchant_locations'
        ]
        
        max_lag = 0
        lag_details = {}
        
        for topic in topics:
            partitions = consumer.partitions_for_topic(topic)
            if not partitions:
                lag_details[topic] = "no partitions"
                continue
            
            tp_list = [TopicPartition(topic, p) for p in partitions]
            consumer.assign(tp_list)
            consumer.poll(timeout_ms=1000)
            
            end_offsets = consumer.end_offsets(tp_list)
            committed = consumer.committed(tp_list)
            
            total_lag = 0
            for tp in tp_list:
                end = end_offsets.get(tp, 0)
                committed_offset = committed.get(tp, 0) or 0
                total_lag += end - committed_offset
            
            lag_details[topic] = total_lag
            max_lag = max(max_lag, total_lag)
        
        consumer.close()
        
        if max_lag > MAX_CDC_LAG:
            return False, f"High CDC lag: {max_lag} (threshold: {MAX_CDC_LAG}), details: {lag_details}"
        
        return True, f"Max lag: {max_lag}, details: {lag_details}"
    except Exception as e:
        return False, f"Kafka lag check failed: {e}"

def check_risingwave():
    """Check RisingWave connectivity and materialized views"""
    try:
        conn = psycopg2.connect(
            host=RISINGWAVE_HOST,
            port=RISINGWAVE_PORT,
            database=RISINGWAVE_DB,
            user=RISINGWAVE_USER,
            password="",
            connect_timeout=5
        )
        
        with conn.cursor() as cur:
            # Check MVs are running
            cur.execute("""
                SELECT name, 'running' as status 
                FROM rw_materialized_views 
                WHERE name IN ('mv_user_velocity_1h', 'mv_user_velocity_7d', 'mv_user_geo_anomaly',
                               'mv_merchant_risk_realtime', 'mv_device_risk_realtime', 'mv_transaction_risk_score')
            """)
            mvs = cur.fetchall()
            
            if len(mvs) < 6:
                return False, f"Only {len(mvs)}/6 MVs found"
            
            # Check data freshness
            cur.execute("""
                SELECT 
                    MAX(_dbt_computed_at) as latest,
                    EXTRACT(EPOCH FROM (NOW() - MAX(_dbt_computed_at)))/60 as min_stale
                FROM gold_fraud_features
            """)
            row = cur.fetchone()
            if row and row[1] and row[1] > MAX_FEATURE_STALE_MIN:
                return False, f"Gold features stale: {row[1]:.1f} min (threshold: {MAX_FEATURE_STALE_MIN})"
            
            # Check CDC freshness
            cur.execute("""
                SELECT 
                    MAX(_cdc_ts) as latest,
                    EXTRACT(EPOCH FROM (NOW() - MAX(_cdc_ts)))/60 as min_stale
                FROM bronze_transactions
            """)
            row = cur.fetchone()
            if row and row[1] and row[1] > MAX_CDC_STALE_MIN:
                return False, f"Bronze CDC stale: {row[1]:.1f} min (threshold: {MAX_CDC_STALE_MIN})"
        
        conn.close()
        return True, f"RisingWave OK: {len(mvs)} MVs running, features fresh"
    except Exception as e:
        return False, f"RisingWave check failed: {e}"

def check_api():
    """Check Fraud Feature API health"""
    try:
        resp = requests.get(f"{API_URL}/health", timeout=5)
        if resp.status_code != 200:
            return False, f"API status: {resp.status_code}"
        
        health = resp.json()
        if health.get('status') != 'healthy':
            return False, f"API unhealthy: {health}"
        
        return True, "API healthy"
    except Exception as e:
        return False, f"API check failed: {e}"

def main():
    """Run all health checks"""
    checks = [
        ("Debezium", check_debezium),
        ("Kafka Lag", check_kafka_lag),
        ("RisingWave", check_risingwave),
        ("Fraud API", check_api),
    ]
    
    results = {}
    all_passed = True
    
    print(f"=== FinPay CDC Health Check - {datetime.utcnow().isoformat()} ===")
    print()
    
    for name, check_fn in checks:
        try:
            passed, message = check_fn()
            results[name] = {"passed": passed, "message": message}
            status = "✅ PASS" if passed else "❌ FAIL"
            print(f"{status} {name}: {message}")
            if not passed:
                all_passed = False
        except Exception as e:
            results[name] = {"passed": False, "message": f"Check crashed: {e}"}
            print(f"❌ FAIL {name}: Check crashed: {e}")
            all_passed = False
    
    print()
    if all_passed:
        print("✅ ALL CHECKS PASSED")
        sys.exit(0)
    else:
        print("❌ SOME CHECKS FAILED")
        sys.exit(1)

if __name__ == "__main__":
    main()