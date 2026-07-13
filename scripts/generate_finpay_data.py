#!/usr/bin/env python3
"""
FinPay Synthetic Data Generator
Generates realistic transaction data with embedded fraud patterns for CDC demo.

Run: python scripts/generate_finpay_data.py --count 50000
"""

import argparse
import random
import uuid
from datetime import datetime, timedelta, timezone
from faker import Faker
import psycopg2
from psycopg2.extras import execute_batch
import sys
import json

fake = Faker()
Faker.seed(42)
random.seed(42)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

DB_CONFIG = {
    "host": "localhost",
    "port": 5433,
    "database": "finpay",
    "user": "finpay",
    "password": "finpay123"
}

# Fraud patterns to inject
FRAUD_PATTERNS = {
    "velocity": 0.02,      # 2% high-velocity bursts
    "geo_mismatch": 0.015, # 1.5% impossible travel
    "amount_outlier": 0.01, # 1% unusual amounts
    "new_device": 0.03,    # 3% first-time device
    "high_risk_merchant": 0.01, # 1% high-risk MCC
    "card_testing": 0.005,  # 0.5% small repeated amounts
}

MCC_CATEGORIES = {
    "grocery": ("5411", "low"),
    "restaurant": ("5812", "low"),
    "gas": ("5541", "low"),
    "retail": ("5311", "low"),
    "online": ("5732", "medium"),
    "travel": ("4511", "medium"),
    "entertainment": ("7941", "medium"),
    "crypto": ("6051", "high"),
    "gambling": ("7995", "high"),
    "adult": ("5967", "high"),
    "money_transfer": ("4829", "critical"),
}

# ISO 3166-1 alpha-2 country codes (2 characters)
COUNTRIES = ["US", "CA", "GB", "DE", "FR", "AU", "JP", "SG", "BR", "MX", "IN", "NG", "RU", "CN"]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_conn():
    return psycopg2.connect(**DB_CONFIG)


def truncate_tables(conn):
    """Clear all tables before generating new data"""
    print("Truncating existing tables...")
    tables = [
        "fraud_signals", "transactions", "devices", 
        "merchant_locations", "merchants", "accounts", "users"
    ]
    with conn.cursor() as cur:
        for table in tables:
            cur.execute(f"TRUNCATE TABLE {table} CASCADE")
    conn.commit()
    print("  Tables truncated")


def weighted_choice(choices):
    """choices: list of (item, weight)"""
    total = sum(w for _, w in choices)
    r = random.uniform(0, total)
    for item, w in choices:
        r -= w
        if r <= 0:
            return item
    return choices[-1][0]

# ---------------------------------------------------------------------------
# Data Generation
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Data Generation
# ---------------------------------------------------------------------------

def generate_users(conn, count):
    print(f"Generating {count} users...")
    users = []
    seen_emails = set()
    for _ in range(count):
        # Generate unique email manually since fake.unique doesn't persist
        while True:
            email = fake.email()
            if email not in seen_emails:
                seen_emails.add(email)
                break
        users.append((
            str(uuid.uuid4()),
            email,
            fake.phone_number()[:20],
            fake.name(),
            random.choices(["pending", "verified", "rejected"], weights=[0.1, 0.85, 0.05])[0],
            random.randint(0, 20),
            datetime.now(timezone.utc) - timedelta(days=random.randint(1, 730)),
            datetime.now(timezone.utc)
        ))
    with conn.cursor() as cur:
        execute_batch(cur, """
            INSERT INTO users (user_id, email, phone, full_name, kyc_status, risk_score, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, users)
    conn.commit()
    print(f"  Inserted {count} users")
    return [u[0] for u in users]

def generate_accounts(conn, user_ids):
    print(f"Generating accounts for {len(user_ids)} users...")
    accounts = []
    for user_id in user_ids:
        n_accounts = random.choices([1, 2, 3], weights=[0.7, 0.25, 0.05])[0]
        for _ in range(n_accounts):
            accounts.append((
                str(uuid.uuid4()),
                user_id,
                random.choices(["checking", "savings", "credit"], weights=[0.6, 0.3, 0.1])[0],
                "USD",
                round(random.uniform(0, 50000), 2),
                "active",
                datetime.now(timezone.utc) - timedelta(days=random.randint(1, 730)),
                None
            ))
    with conn.cursor() as cur:
        execute_batch(cur, """
            INSERT INTO accounts (account_id, user_id, account_type, currency, balance, status, opened_at, closed_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, accounts)
    conn.commit()
    print(f"  Inserted {len(accounts)} accounts")
    return [a[0] for a in accounts]

def generate_merchants(conn, count=500):
    print(f"Generating {count} merchants...")
    merchants = []
    for _ in range(count):
        cat = random.choice(list(MCC_CATEGORIES.keys()))
        mcc, risk = MCC_CATEGORIES[cat]
        merchants.append((
            str(uuid.uuid4()),
            fake.company(),
            cat,
            mcc,
            random.choice(COUNTRIES),
            fake.city(),
            risk,
            datetime.now(timezone.utc) - timedelta(days=random.randint(1, 1095)),
            random.random() > 0.02
        ))
    with conn.cursor() as cur:
        execute_batch(cur, """
            INSERT INTO merchants (merchant_id, name, category, mcc_code, country, city, risk_level, onboarded_at, is_active)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, merchants)
    conn.commit()
    print(f"  Inserted {count} merchants")
    return [m[0] for m in merchants]

def generate_merchant_locations(conn, merchant_ids):
    print(f"Generating locations for {len(merchant_ids)} merchants...")
    locations = []
    for mid in merchant_ids:
        n_loc = random.choices([1, 2, 3, 5], weights=[0.6, 0.2, 0.15, 0.05])[0]
        for _ in range(n_loc):
            country = random.choice(COUNTRIES)
            is_online = random.random() < 0.3
            loc_country = country if not is_online else "OL"  # "OL" = online
            locations.append((
                str(uuid.uuid4()),
                mid,
                fake.bothify(text='TERM-####??').upper(),
                loc_country,
                fake.city() if not is_online else "INTERNET",
                round(random.uniform(-90, 90), 8) if not is_online else None,
                round(random.uniform(-180, 180), 8) if not is_online else None,
                is_online
            ))
    with conn.cursor() as cur:
        execute_batch(cur, """
            INSERT INTO merchant_locations (location_id, merchant_id, terminal_id, country, city, lat, lon, is_online)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, locations)
    conn.commit()
    print(f"  Inserted {len(locations)} locations")
    return [l[0] for l in locations]

def generate_devices(conn, user_ids):
    print(f"Generating devices for {len(user_ids)} users...")
    devices = []
    for uid in user_ids:
        n_devices = random.choices([1, 2, 3], weights=[0.7, 0.25, 0.05])[0]
        for i in range(n_devices):
            devices.append((
                str(uuid.uuid4()),
                uid,
                fake.sha256()[:32],
                random.choices(["mobile", "desktop", "tablet"], weights=[0.6, 0.3, 0.1])[0],
                random.choice(["iOS", "Android", "Windows", "macOS", "Linux"]),
                random.choice(["Chrome", "Safari", "Firefox", "Edge", "App"]),
                fake.ipv4(),
                random.choice(COUNTRIES),
                fake.city(),
                round(random.uniform(-90, 90), 8),
                round(random.uniform(-180, 180), 8),
                i == 0,  # first device is trusted
                datetime.now(timezone.utc) - timedelta(days=random.randint(1, 730)),
                datetime.now(timezone.utc)
            ))
    with conn.cursor() as cur:
        execute_batch(cur, """
            INSERT INTO devices (device_id, user_id, device_fingerprint, device_type, os, browser, ip_address, country, city, lat, lon, is_trusted, first_seen_at, last_seen_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, devices)
    conn.commit()
    print(f"  Inserted {len(devices)} devices")
    return {uid: [d[0] for d in devices if d[1] == uid] for uid in user_ids}

def generate_transactions(conn, account_ids, user_id_by_account, merchant_ids, location_ids, user_devices, count):
    print(f"Generating {count} transactions...")
    batch_size = 1000
    txns = []
    
    # Pre-fetch merchant risk levels
    with conn.cursor() as cur:
        cur.execute("SELECT merchant_id, risk_level, category FROM merchants")
        merchant_info = {row[0]: {"risk": row[1], "cat": row[2]} for row in cur.fetchall()}
    
    location_info = {}
    with conn.cursor() as cur:
        cur.execute("SELECT location_id, country, city, lat, lon, is_online FROM merchant_locations")
        location_info = {row[0]: {"country": row[1], "city": row[2], "lat": row[3], "lon": row[4], "online": row[5]} for row in cur.fetchall()}

    user_last_txn = {}  # for velocity patterns
    user_last_location = {}  # for geo mismatch

    for i in range(count):
        account_id = random.choice(account_ids)
        user_id = user_id_by_account[account_id]
        
        # Pick time - weighted toward recent
        days_ago = random.choices(
            [0, 1, 2, 3, 4, 5, 6, 7, 14, 30],
            weights=[0.3, 0.15, 0.1, 0.1, 0.08, 0.07, 0.06, 0.05, 0.05, 0.04]
        )[0]
        hours_ago = random.randint(0, 23)
        minutes_ago = random.randint(0, 59)
        initiated_at = datetime.now(timezone.utc) - timedelta(days=days_ago, hours=hours_ago, minutes=minutes_ago)
        
        # Merchant selection - sometimes inject high-risk
        if random.random() < FRAUD_PATTERNS["high_risk_merchant"]:
            high_risk_merchants = [m for m, info in merchant_info.items() if info["risk"] in ("high", "critical")]
            merchant_id = random.choice(high_risk_merchants) if high_risk_merchants else random.choice(merchant_ids)
        else:
            merchant_id = random.choice(merchant_ids)
        
        m_info = merchant_info[merchant_id]
        
        # Location
        merchant_locs = [lid for lid in location_ids if location_info[lid].get("merchant_id") == merchant_id]
        # Need to get merchant locations - simplified
        location_id = random.choice(location_ids)
        loc_info = location_info[location_id]
        
        # Device
        user_device_ids = user_devices.get(user_id, [])
        if random.random() < FRAUD_PATTERNS["new_device"]:
            device_id = str(uuid.uuid4())  # New unseen device
        else:
            device_id = random.choice(user_device_ids) if user_device_ids else str(uuid.uuid4())
        
        # Amount - sometimes inject outliers
        if random.random() < FRAUD_PATTERNS["amount_outlier"]:
            amount = round(random.uniform(5000, 50000), 2)
        elif m_info["cat"] == "gas":
            amount = round(random.uniform(20, 150), 2)
        elif m_info["cat"] == "grocery":
            amount = round(random.uniform(10, 300), 2)
        elif m_info["cat"] in ("crypto", "gambling", "money_transfer"):
            amount = round(random.uniform(100, 10000), 2)
        else:
            amount = round(random.uniform(5, 500), 2)
        
        # Card testing pattern - many small amounts
        if random.random() < FRAUD_PATTERNS["card_testing"]:
            amount = round(random.uniform(0.5, 5.0), 2)
        
        # Velocity pattern - burst of transactions
        is_velocity = False
        if user_id in user_last_txn:
            time_diff = (initiated_at - user_last_txn[user_id]).total_seconds()
            if time_diff < 60 and random.random() < FRAUD_PATTERNS["velocity"]:
                is_velocity = True
        
        user_last_txn[user_id] = initiated_at
        
        # Geo mismatch
        is_geo_mismatch = False
        if user_id in user_last_location and not loc_info["online"]:
            last_loc = user_last_location[user_id]
            if last_loc and loc_info["lat"] and last_loc["lat"]:
                # Simplified: if countries differ and time < 4 hours
                if last_loc["country"] != loc_info["country"]:
                    time_diff = (initiated_at - last_loc["time"]).total_seconds()
                    if time_diff < 14400 and random.random() < FRAUD_PATTERNS["geo_mismatch"]:
                        is_geo_mismatch = True
        
        user_last_location[user_id] = {"country": loc_info["country"], "time": initiated_at, "lat": loc_info["lat"], "lon": loc_info["lon"]}
        
        # Transaction type
        txn_type = "purchase"
        if m_info["cat"] in ("crypto", "money_transfer"):
            txn_type = random.choice(["purchase", "transfer"])
        
        # Status
        txn_status = random.choices(
            ["authorized", "settled", "declined", "pending"],
            weights=[0.45, 0.45, 0.08, 0.02]
        )[0]
        
        # Entry mode
        entry_mode = "online" if loc_info["online"] else random.choices(
            ["chip", "contactless", "swipe"], weights=[0.5, 0.4, 0.1]
        )[0]
        
        txns.append((
            str(uuid.uuid4()),
            account_id,
            user_id,
            merchant_id,
            location_id,
            device_id,
            amount,
            "USD",
            txn_type,
            txn_status,
            entry_mode,
            not loc_info["online"],
            random.random() < 0.3,
            random.random() < 0.2,
            fake.sentence(nb_words=6),
            json.dumps({"source": "generator", "velocity_flag": is_velocity, "geo_flag": is_geo_mismatch}),
            initiated_at,
            initiated_at + timedelta(seconds=random.randint(1, 30)) if txn_status != "pending" else None,
            initiated_at + timedelta(days=random.randint(1, 2)) if txn_status == "settled" else None,
            initiated_at,
            initiated_at
        ))
        
        if len(txns) >= batch_size:
            with conn.cursor() as cur:
                execute_batch(cur, """
                    INSERT INTO transactions (transaction_id, account_id, user_id, merchant_id, location_id, device_id,
                        amount, currency, txn_type, txn_status, entry_mode, card_present, is_3ds, is_tokenized,
                        description, metadata, initiated_at, authorized_at, settled_at, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, txns)
            conn.commit()
            txns = []
            
        if (i + 1) % 10000 == 0:
            print(f"  Generated {i + 1}/{count} transactions...")

    if txns:
        with conn.cursor() as cur:
            execute_batch(cur, """
                INSERT INTO transactions (transaction_id, account_id, user_id, merchant_id, location_id, device_id,
                    amount, currency, txn_type, txn_status, entry_mode, card_present, is_3ds, is_tokenized,
                    description, metadata, initiated_at, authorized_at, settled_at, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, txns)
        conn.commit()
    
    print(f"  Inserted {count} transactions")


def generate_transactions_v2(conn, account_ids, user_id_by_account, merchant_ids, location_info, device_ids_by_user, all_device_ids, count):
    """Generate transactions with correct foreign key references"""
    print(f"Generating {count} transactions...")
    batch_size = 1000
    txns = []
    
    # Pre-fetch merchant info
    merchant_info = {}
    with conn.cursor() as cur:
        cur.execute("SELECT merchant_id, risk_level, category FROM merchants")
        merchant_info = {row[0]: {"risk": row[1], "cat": row[2]} for row in cur.fetchall()}
    
    # Build location -> merchant mapping
    loc_to_merchant = {loc_id: info["merchant_id"] for loc_id, info in location_info.items()}
    
    user_last_txn = {}
    user_last_location = {}
    
    for i in range(count):
        account_id = random.choice(account_ids)
        user_id = user_id_by_account[account_id]
        
        # Pick time - weighted toward recent
        days_ago = random.choices(
            [0, 1, 2, 3, 4, 5, 6, 7, 14, 30],
            weights=[0.3, 0.15, 0.1, 0.1, 0.08, 0.07, 0.06, 0.05, 0.05, 0.04]
        )[0]
        hours_ago = random.randint(0, 23)
        minutes_ago = random.randint(0, 59)
        initiated_at = datetime.now(timezone.utc) - timedelta(days=days_ago, hours=hours_ago, minutes=minutes_ago)
        
        # Merchant selection
        if random.random() < FRAUD_PATTERNS["high_risk_merchant"]:
            high_risk_merchants = [m for m, info in merchant_info.items() if info["risk"] in ("high", "critical")]
            merchant_id = random.choice(high_risk_merchants) if high_risk_merchants else random.choice(list(merchant_info.keys()))
        else:
            merchant_id = random.choice(list(merchant_info.keys()))
        
        m_info = merchant_info[merchant_id]
        
        # Location - pick one that belongs to this merchant
        merchant_locs = [lid for lid, info in location_info.items() if info.get("merchant_id") == merchant_id]
        location_id = random.choice(merchant_locs) if merchant_locs else random.choice(list(location_info.keys()))
        loc_info = location_info[location_id]
        
        # Device - pick from user's devices (always use existing device to satisfy FK)
        user_device_ids = device_ids_by_user.get(user_id, [])
        if user_device_ids:
            device_id = random.choice(user_device_ids)
        else:
            device_id = random.choice(all_device_ids)
        
        # Amount
        if random.random() < FRAUD_PATTERNS["amount_outlier"]:
            amount = round(random.uniform(5000, 50000), 2)
        elif m_info["cat"] == "gas":
            amount = round(random.uniform(20, 150), 2)
        elif m_info["cat"] == "grocery":
            amount = round(random.uniform(10, 300), 2)
        elif m_info["cat"] in ("crypto", "gambling", "money_transfer"):
            amount = round(random.uniform(100, 10000), 2)
        else:
            amount = round(random.uniform(5, 500), 2)
        
        if random.random() < FRAUD_PATTERNS["card_testing"]:
            amount = round(random.uniform(0.5, 5.0), 2)
        
        # Velocity pattern
        is_velocity = False
        if user_id in user_last_txn:
            time_diff = (initiated_at - user_last_txn[user_id]).total_seconds()
            if time_diff < 60 and random.random() < FRAUD_PATTERNS["velocity"]:
                is_velocity = True
        
        user_last_txn[user_id] = initiated_at
        
        # Geo mismatch
        is_geo_mismatch = False
        if user_id in user_last_location and not loc_info.get("online"):
            last_loc = user_last_location[user_id]
            if last_loc and loc_info.get("lat") and last_loc.get("lat"):
                if last_loc["country"] != loc_info.get("country"):
                    time_diff = (initiated_at - last_loc["time"]).total_seconds()
                    if time_diff < 14400 and random.random() < FRAUD_PATTERNS["geo_mismatch"]:
                        is_geo_mismatch = True
        
        user_last_location[user_id] = {"country": loc_info.get("country"), "time": initiated_at, "lat": loc_info.get("lat"), "lon": loc_info.get("lon")}
        
        # Transaction type
        txn_type = "purchase"
        if m_info["cat"] in ("crypto", "money_transfer"):
            txn_type = random.choice(["purchase", "transfer"])
        
        # Status
        txn_status = random.choices(
            ["authorized", "settled", "declined", "pending"],
            weights=[0.45, 0.45, 0.08, 0.02]
        )[0]
        
        # Entry mode
        entry_mode = "online" if loc_info.get("online") else random.choices(
            ["chip", "contactless", "swipe"], weights=[0.5, 0.4, 0.1]
        )[0]
        
        txns.append((
            str(uuid.uuid4()),
            account_id,
            user_id,
            merchant_id,
            location_id,
            device_id,
            amount,
            "USD",
            txn_type,
            txn_status,
            entry_mode,
            not loc_info.get("online"),
            random.random() < 0.3,
            random.random() < 0.2,
            fake.sentence(nb_words=6),
            json.dumps({"source": "generator", "velocity_flag": is_velocity, "geo_flag": is_geo_mismatch}),
            initiated_at,
            initiated_at + timedelta(seconds=random.randint(1, 30)) if txn_status != "pending" else None,
            initiated_at + timedelta(days=random.randint(1, 2)) if txn_status == "settled" else None,
            initiated_at,
            initiated_at
        ))
        
        if len(txns) >= batch_size:
            with conn.cursor() as cur:
                execute_batch(cur, """
                    INSERT INTO transactions (transaction_id, account_id, user_id, merchant_id, location_id, device_id,
                        amount, currency, txn_type, txn_status, entry_mode, card_present, is_3ds, is_tokenized,
                        description, metadata, initiated_at, authorized_at, settled_at, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, txns)
            conn.commit()
            txns = []
            
        if (i + 1) % 10000 == 0:
            print(f"  Generated {i + 1}/{count} transactions...")

    if txns:
        with conn.cursor() as cur:
            execute_batch(cur, """
                INSERT INTO transactions (transaction_id, account_id, user_id, merchant_id, location_id, device_id,
                    amount, currency, txn_type, txn_status, entry_mode, card_present, is_3ds, is_tokenized,
                    description, metadata, initiated_at, authorized_at, settled_at, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, txns)
        conn.commit()
    
    print(f"  Inserted {count} transactions")


def main():
    parser = argparse.ArgumentParser(description="Generate FinPay synthetic data")
    parser.add_argument("--users", type=int, default=1000, help="Number of users")
    parser.add_argument("--merchants", type=int, default=500, help="Number of merchants")
    parser.add_argument("--transactions", type=int, default=50000, help="Number of transactions")
    parser.add_argument("--host", default="localhost", help="Postgres host")
    parser.add_argument("--port", type=int, default=5433, help="Postgres port")
    args = parser.parse_args()
    
    DB_CONFIG["host"] = args.host
    DB_CONFIG["port"] = args.port
    
    print("Connecting to FinPay database...")
    try:
        conn = get_conn()
        conn.autocommit = False
    except Exception as e:
        print(f"Failed to connect: {e}")
        print("Make sure FinPay Postgres is running on port 5433")
        sys.exit(1)
    
    try:
        # Clear existing data first
        truncate_tables(conn)
        
        # Generate in dependency order
        user_ids = generate_users(conn, args.users)
        
        account_ids = generate_accounts(conn, user_ids)
        user_id_by_account = {}
        with conn.cursor() as cur:
            cur.execute("SELECT account_id, user_id FROM accounts")
            user_id_by_account = {row[0]: row[1] for row in cur.fetchall()}
        
        merchant_ids = generate_merchants(conn, args.merchants)
        location_ids = generate_merchant_locations(conn, merchant_ids)
        
        # Pre-fetch location info for transaction generation
        location_info = {}
        with conn.cursor() as cur:
            cur.execute("SELECT location_id, merchant_id, country, city, lat, lon, is_online FROM merchant_locations")
            location_info = {row[0]: {"merchant_id": row[1], "country": row[2], "city": row[3], "lat": row[4], "lon": row[5], "online": row[6]} for row in cur.fetchall()}
        
        device_ids_by_user = generate_devices(conn, user_ids)
        all_device_ids = []
        for devs in device_ids_by_user.values():
            all_device_ids.extend(devs)
        
        # Generate transactions with correct references
        generate_transactions_v2(conn, account_ids, user_id_by_account, merchant_ids, location_info, device_ids_by_user, all_device_ids, args.transactions)
        
    finally:
        conn.close()

if __name__ == "__main__":
    main()