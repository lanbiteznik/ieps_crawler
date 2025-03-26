import psycopg2
import argparse

def clear_database_data(host="localhost", port=5432, dbname="wier", user="postgres", password="admin"):
    """Delete all data from all tables in the crawldb schema while preserving table structure"""
    try:
        # Connect to the database
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password
        )
        conn.autocommit = True
        cursor = conn.cursor()
        
        print("Connected to database. Preparing to clear all data...")
        
        # Disable foreign key checks temporarily
        cursor.execute("SET session_replication_role = 'replica';")

        # Tables to clear data from (in proper order to handle dependencies)
        tables = ["link", "image", "page_data", "page", "site"]
        
        # Truncate each table (RESTART IDENTITY resets sequence counters)
        for table in tables:
            print(f"Clearing data from crawldb.{table}...")
            cursor.execute(f"TRUNCATE TABLE crawldb.{table} RESTART IDENTITY CASCADE;")
        
        # Re-enable foreign key checks
        cursor.execute("SET session_replication_role = 'origin';")
        
        print("Database data cleared successfully!")
        
        # Print table counts to confirm empty
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM crawldb.{table}")
            count = cursor.fetchone()[0]
            print(f"crawldb.{table}: {count} rows")
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        print(f"Error clearing database data: {e}")
        return False

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Clear all data from the crawler database tables")
    parser.add_argument('--host', default='localhost', help='Database host')
    parser.add_argument('--port', type=int, default=5432, help='Database port')
    parser.add_argument('--db', default='wier', help='Database name')
    parser.add_argument('--user', default='postgres', help='Database user')
    parser.add_argument('--password', default='admin', help='Database password')
    parser.add_argument('--yes', action='store_true', help='Skip confirmation prompt')
    
    args = parser.parse_args()
    
    if not args.yes:
        confirmation = input("WARNING: This will delete ALL DATA from your crawler database tables.\n"
                           "The table structure will be preserved but all rows will be deleted.\n"
                           "Type 'CLEAR' to confirm: ")
        if confirmation != "CLEAR":
            print("Operation cancelled.")
            exit()
    
    clear_database_data(args.host, args.port, args.db, args.user, args.password)