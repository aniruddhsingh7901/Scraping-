#!/usr/bin/env python3
"""Quick check of target database"""
import psycopg2

try:
    conn = psycopg2.connect(
        host="localhost",
        port="5432",
        user="reddit_user",
        password="postgres",
        database="reddit_miner_db"
    )
    
    with conn.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM dataentity")
        count = cursor.fetchone()[0]
        print(f"Records in reddit_miner_db.dataentity: {count:,}")
        
        if count > 0:
            cursor.execute("SELECT uri, datetime FROM dataentity ORDER BY datetime DESC LIMIT 3")
            print("\nLatest 3 records:")
            for uri, dt in cursor.fetchall():
                print(f"  - {dt}: {uri}")
        else:
            print("⚠️  No records found in dataentity table!")
    
    conn.close()
except Exception as e:
    print(f"Error: {e}")
