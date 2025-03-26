import psycopg2
import argparse
import time
from datetime import datetime, timedelta
import os

# Include the debug functions from above

def main():
    parser = argparse.ArgumentParser(description='Debug web crawler system')
    parser.add_argument('--component', choices=['database', 'crawler', 'monitoring', 'all'], 
                        default='all', help='Which component to debug')
    args = parser.parse_args()
    
    print("===== Crawler Debug Tool =====")
    print(f"Current time: {datetime.now()}")
    
    if args.component in ['database', 'all']:
        print("\n[DATABASE CONNECTION TEST]")
        test_database_connection()
    
    if args.component in ['crawler', 'all']:
        print("\n[CRAWLER STATUS CHECK]")
        debug_crawler_status()
    
    if args.component in ['monitoring', 'all']:
        print("\n[MONITORING QUERIES TEST]")
        debug_monitoring_queries()

if __name__ == '__main__':
    main()

def test_database_connection():
    """Test the database connection with detailed error reporting"""
    try:
        # Try different database credentials
        connection_params = [
            {"host": "localhost", "port": 5432, "database": "wier", 
             "user": "postgres", "password": "admin"}
        ]
        
        for params in connection_params:
            try:
                print(f"Trying connection with: {params}")
                conn = psycopg2.connect(**params)
                
                # Test basic queries on each table
                cursor = conn.cursor()
                tables = ["page", "site", "page_data", "link", "image"]
                
                for table in tables:
                    try:
                        cursor.execute(f"SELECT COUNT(*) FROM crawldb.{table}")
                        count = cursor.fetchone()[0]
                        print(f"  ✓ Table crawldb.{table} exists with {count} rows")
                    except Exception as e:
                        print(f"  ✗ Error accessing table crawldb.{table}: {e}")
                
                cursor.close()
                conn.close()
                print("✅ Connection successful!")
                return True
            except Exception as e:
                print(f"❌ Connection failed: {e}")
        
        return False
    except Exception as e:
        print(f"Fatal error: {e}")
        return False
    
def debug_crawler_status():
    """Check crawler status and identify common issues"""
    try:
        conn = psycopg2.connect(
            host="localhost", port=5432, database="wier", 
            user="postgres", password="admin"
        )
        cursor = conn.cursor()
        
        # Check if seed URLs were added
        print("Checking seed URLs...")
        cursor.execute("""
            SELECT COUNT(*) FROM crawldb.page 
            WHERE url LIKE 'https://med.over.net/%'
        """)
        seed_count = cursor.fetchone()[0]
        print(f"  Found {seed_count} med.over.net URLs in database")
        
        # Check for recent activity
        print("\nChecking crawler activity...")
        cursor.execute("""
            SELECT COUNT(*), MAX(accessed_time)
            FROM crawldb.page
            WHERE page_type_code = 'HTML' 
            AND accessed_time > NOW() - INTERVAL '10 minutes'
        """)
        recent_count, last_time = cursor.fetchone()
        print(f"  Pages crawled in last 10 minutes: {recent_count}")
        print(f"  Last page access time: {last_time}")
        
        # Check for errors (HTTP status codes)
        print("\nChecking for HTTP errors...")
        cursor.execute("""
            SELECT http_status_code, COUNT(*)
            FROM crawldb.page
            WHERE http_status_code IS NOT NULL
            AND http_status_code != 200
            GROUP BY http_status_code
            ORDER BY COUNT(*) DESC
        """)
        for status, count in cursor.fetchall():
            print(f"  HTTP {status}: {count} pages")
        
        # Check for duplicate detection
        print("\nChecking duplicate detection...")
        cursor.execute("""
            SELECT COUNT(*) FROM crawldb.page
            WHERE page_type_code = 'DUPLICATE'
        """)
        duplicate_count = cursor.fetchone()[0]
        print(f"  Duplicates detected: {duplicate_count}")
        
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Error checking crawler status: {e}")

def debug_monitoring_queries():
    """Test each monitoring query separately to find issues"""
    try:
        conn = psycopg2.connect(
            host="localhost", port=5432, database="wier", 
            user="postgres", password="admin"
        )
        cursor = conn.cursor()
        
        queries = [
            ("Page type counts", 
             "SELECT page_type_code, COUNT(*) FROM crawldb.page GROUP BY page_type_code"),
            
            ("Domain counts", 
             """SELECT s.domain, COUNT(p.id) AS page_count
                FROM crawldb.site s
                JOIN crawldb.page p ON p.site_id = s.id
                WHERE p.page_type_code = 'HTML'
                GROUP BY s.domain
                ORDER BY page_count DESC
                LIMIT 5"""),
            
            ("Image count", 
             "SELECT COUNT(*) FROM crawldb.image"),
            
            ("Link count", 
             "SELECT COUNT(*) FROM crawldb.link"),
            
            ("Crawl timespan", 
             """SELECT MIN(accessed_time), MAX(accessed_time), COUNT(*)
                FROM crawldb.page
                WHERE page_type_code = 'HTML' AND accessed_time IS NOT NULL"""),
            
            ("Recent crawl rate", 
             """SELECT COUNT(*) FROM crawldb.page 
                WHERE page_type_code = 'HTML' 
                AND accessed_time > NOW() - INTERVAL '5 minutes'""")
        ]
        
        for description, query in queries:
            print(f"\nTesting query: {description}")
            try:
                cursor.execute(query)
                result = cursor.fetchall()
                print(f"  ✓ Success: {result}")
            except Exception as e:
                print(f"  ✗ Error: {e}")
        
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Connection error: {e}")