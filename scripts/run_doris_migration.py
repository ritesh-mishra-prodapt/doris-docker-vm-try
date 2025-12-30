#!/usr/bin/env python3
"""
Run migration SQL against a Doris FE MySQL endpoint.

Usage:
  pip install -r requirements.txt
  python scripts/run_doris_migration.py --host 127.0.0.1 --port 9030 --user root

The script will wait for the FE MySQL port, execute the SQL statements, print results for SELECTs
and errors if any statement fails.
"""

import time
import argparse
import pymysql
import sys

SQL_STATEMENTS = [
    "CREATE DATABASE IF NOT EXISTS migration_test;",
    "USE migration_test;",
    (
        "CREATE TABLE IF NOT EXISTS agent_test_table (\n"
        "    id INT,\n"
        "    name VARCHAR(50)\n"
        ") \n"
        "DISTRIBUTED BY HASH(id) BUCKETS 1 \n"
        "PROPERTIES(\"replication_num\" = \"1\");"
    ),
    "INSERT INTO agent_test_table VALUES (1, 'Bq2Doris_Success');",
    "SELECT id, name FROM agent_test_table;",
]


def wait_for_mysql(conn_args, timeout=60, interval=2):
    start = time.time()
    while time.time() - start < timeout:
        try:
            conn = pymysql.connect(**conn_args)
            conn.close()
            return True
        except Exception as e:
            print(f"Waiting for FE MySQL to be ready: {e}")
            time.sleep(interval)
    return False


def run_statements(conn_args):
    try:
        conn = pymysql.connect(**conn_args, autocommit=True)
    except Exception as e:
        print(f"Failed to connect: {e}")
        return 2

    try:
        cur = conn.cursor()
        for sql in SQL_STATEMENTS:
            sql_strip = sql.strip()
            print('\n---> Executing SQL:\n' + sql_strip)
            try:
                cur.execute(sql)
            except Exception as e:
                print(f"ERROR executing statement: {e}")
                # continue so we can see later statements (adjust behavior as needed)
                continue

            if sql_strip.upper().startswith("SELECT"):
                rows = cur.fetchall()
                print(f"Query returned {len(rows)} row(s):")
                for r in rows:
                    print(r)

        return 0
    finally:
        try:
            cur.close()
        except Exception:
            pass
        conn.close()


def parse_args():
    p = argparse.ArgumentParser(description="Run migration SQL against Doris FE MySQL port")
    p.add_argument("--host", default="127.0.0.1", help="FE MySQL host")
    p.add_argument("--port", default=9030, type=int, help="FE MySQL port (default: 9030)")
    p.add_argument("--user", default="root", help="MySQL user (default: root)")
    p.add_argument("--password", default="", help="MySQL password (if any)")
    p.add_argument("--timeout", default=60, type=int, help="Seconds to wait for FE MySQL to accept connections")
    return p.parse_args()


def main():
    args = parse_args()
    conn_args = {"host": args.host, "port": args.port, "user": args.user, "password": args.password, "charset": "utf8mb4", "cursorclass": pymysql.cursors.Cursor}

    print(f"Waiting up to {args.timeout}s for FE MySQL at {args.host}:{args.port}...")
    if not wait_for_mysql(conn_args, timeout=args.timeout):
        print("FE MySQL did not become ready in time. Check `docker compose ps` and FE logs.")
        sys.exit(3)

    rc = run_statements(conn_args)
    if rc == 0:
        print("\nAll done.")
    sys.exit(rc)


if __name__ == "__main__":
    main()
