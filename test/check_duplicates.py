import os
import sys
import argparse
from datetime import datetime

# Add the parent directory to the path so we can import modules properly
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from crawler import Crawler
    from database import Database
except ImportError as e:
    print(f"Error importing required modules: {e}")
    print("Make sure you're running this script from the test directory or project root.")
    sys.exit(1)

def main():
    """
    Run duplicate detection on all HTML pages in the database.
    This helps find and mark duplicate pages that might have been missed during crawling.
    """
    parser = argparse.ArgumentParser(description='Find duplicate pages in the crawl database')
    parser.add_argument('--verbose', '-v', action='store_true', 
                        help='Increase output verbosity')
    args = parser.parse_args()
    
    try:
        print(f"Starting duplicate detection at: {datetime.now()}")
        print("Connecting to database...")
        db = Database()
        
        print("Checking for duplicate pages...")
        crawler = Crawler([])
        crawler.db = db
        crawler.find_duplicates()
        
        # Query count of duplicates after processing
        cursor = db.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM crawldb.page WHERE page_type_code = 'DUPLICATE'")
        duplicate_count = cursor.fetchone()[0]
        cursor.close()
        
        print(f"Duplicate detection completed. Found {duplicate_count} duplicate pages.")
        print(f"Finished at: {datetime.now()}")
        
    except Exception as e:
        print(f"Error during duplicate detection: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())