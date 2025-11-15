#!/usr/bin/env python3
"""
cleanup_db_connections.py

Check and optionally close idle PostgreSQL connections
"""

import psycopg2
import sys

def cleanup_connections(host="localhost", port="5432", user="postgres", password="password", database="postgres"):
    """Clean up idle connections"""
    try:
        # Connect to postgres database (not reddit or reddit_miner_db)
        conn = psycopg2.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,  # Connect to postgres database
            connect_timeout=10
        )
        conn.autocommit = True
        
        with conn.cursor() as cursor:
            # Show current connections
            print("\n=== Current Database Connections ===")
            cursor.execute("""
                SELECT pid, datname, usename, application_name, state, 
                       state_change, query_start
                FROM pg_stat_activity
                WHERE pid <> pg_backend_pid()
                ORDER BY state_change
            """)
            
            connections = cursor.fetchall()
            print(f"Total active connections: {len(connections)}\n")
            
            idle_count = 0
            for conn_info in connections:
                pid, db, user, app, state, state_change, query_start = conn_info
                if state == 'idle':
                    idle_count += 1
                    print(f"PID: {pid}, DB: {db}, User: {user}, State: {state}, App: {app}")
            
            print(f"\nTotal idle connections: {idle_count}")
            
            # Check max connections setting
            cursor.execute("SHOW max_connections")
            max_conn = cursor.fetchone()[0]
            print(f"Max connections allowed: {max_conn}")
            
            # Ask to terminate idle connections
            if idle_count > 0:
                response = input(f"\nTerminate {idle_count} idle connections? (yes/no): ").strip().lower()
                if response == 'yes':
                    cursor.execute("""
                        SELECT pg_terminate_backend(pid)
                        FROM pg_stat_activity
                        WHERE state = 'idle'
                          AND pid <> pg_backend_pid()
                          AND datname IN ('reddit', 'reddit_miner_db')
                    """)
                    terminated = cursor.rowcount
                    print(f"✓ Terminated {terminated} idle connections")
                else:
                    print("No connections terminated")
        
        conn.close()
        print("\n✓ Done")
        
    except Exception as e:
        print(f"Error: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(cleanup_connections())
