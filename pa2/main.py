import psycopg2
from psycopg2.extras import RealDictCursor
from typing import Optional, Dict, Any
from html_cleaner import HTMLCleaner
from vector_processor import VectorProcessor
import os
import dotenv
from tqdm import tqdm
import time
from datetime import datetime, timedelta

dotenv.load_dotenv(override=True)
db_name = os.getenv("DB_NAME")
print(f"DB_NAME: {db_name}")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")

class DatabaseManager:
    def __init__(self,
                host: str = "localhost",
                port: str = "5432",
                database: str = "IEPSdb_partial",
                user: str = "postgres",
                password: str = "Admin"):
        
        """Initialize database connection parameters."""
        self.connection_params = {
            "host": host,
            "port": port,
            "database": database,
            "user": user,
            "password": password,
            "cursor_factory": RealDictCursor
        }
        
    def get_connection(self):
        """Create and return a database connection."""
        return psycopg2.connect(**self.connection_params)
    
def estimate_completion_time(start_time: float, processed: int, total: int) -> str:
    """Calculate estimated time remaining based on current progress."""
    if processed == 0:
        return "Calculating..."
    
    elapsed = time.time() - start_time
    pages_per_second = processed / elapsed
    remaining_seconds = (total - processed) / pages_per_second
    
    return str(timedelta(seconds=int(remaining_seconds)))

def main():
    """Main function to process HTML pages and create vector embeddings."""
    try:
        print("Initializing components...")
        html_cleaner = HTMLCleaner()
        vector_processor = VectorProcessor()
        
        # Connect to database
        conn = DatabaseManager(db_host, db_port, db_name, db_user, db_password).get_connection()
        conn.autocommit = True
        cursor = conn.cursor()
        
        print("Setting up database tables...")
        #cursor.execute("""
        #               DROP TABLE IF EXISTS crawldb.cleaned_page;
        #              DROP TABLE IF EXISTS crawldb.page_segment;
        #              """)
        
        cursor.execute("""
            CREATE SCHEMA IF NOT EXISTS crawldb;

            CREATE TABLE IF NOT EXISTS crawldb.cleaned_page (
                id INTEGER PRIMARY KEY,
                url TEXT NOT NULL,
                plain_text TEXT,
                block_system BOOLEAN DEFAULT FALSE
            );
            
            CREATE TABLE IF NOT EXISTS crawldb.page_segment (
                id SERIAL PRIMARY KEY,
                page_id INTEGER NOT NULL REFERENCES crawldb.cleaned_page(id) ON DELETE CASCADE,
                page_segment TEXT NOT NULL,
                embedding VECTOR(768)
            );
        """)
        
        # Fetch pages that need processing
        print("Fetching pages from database...")
        cursor.execute("""
            SELECT id, url, html_content 
            FROM crawldb.page 
            WHERE html_content IS NOT NULL;
        """)
        pages = cursor.fetchall()
        total_pages = len(pages)
        
        print(f"\nStarting processing of {total_pages} pages...")
        start_time = time.time()
        processed_pages = 0
        successful_pages = 0
        failed_pages = 0

        # Create progress bar
        with tqdm(total=total_pages, desc="Processing pages", unit="page") as pbar:
            for page in pages:
                try:
                    # Clean HTML
                    clean_text, used_block = html_cleaner.clean_html(page['html_content'])
                    
                    if clean_text:
                        # Store cleaned text
                        cursor.execute("""
                            INSERT INTO crawldb.cleaned_page (id, url, plain_text, block_system)
                            VALUES (%s, %s, %s, %s)
                            ON CONFLICT (id) DO UPDATE
                            SET plain_text = EXCLUDED.plain_text,
                                block_system = EXCLUDED.block_system;
                        """, (page['id'], page['url'], clean_text, used_block))
                        
                        # Create segments
                        segments = [
                            {'id': None, 'text': segment.strip()}
                            for segment in clean_text.split(HTMLCleaner.PARAGRAPH_BREAK)
                            if segment.strip()
                        ]
                        
                        # Process segments and get embeddings
                        processed_segments = vector_processor.process_segments(segments)
                        
                        # Store segments with embeddings
                        for segment in processed_segments:
                            cursor.execute("""
                                INSERT INTO crawldb.page_segment (page_id, page_segment, embedding)
                                VALUES (%s, %s, %s);
                            """, (page['id'], segment['text'], segment['embedding']))
                            
                        successful_pages += 1
                    
                except Exception as e:
                    print(f"\nError processing page {page['id']}: {str(e)}")
                    failed_pages += 1
                
                processed_pages += 1
                
                # Update progress bar with current stats
                remaining_time = estimate_completion_time(start_time, processed_pages, total_pages)
                pbar.set_postfix({
                    'successful': successful_pages,
                    'failed': failed_pages,
                    'remaining_time': remaining_time
                })
                pbar.update(1)
                
        # Print final statistics
        total_time = time.time() - start_time
        print(f"\nProcessing completed in {timedelta(seconds=int(total_time))}!")
        print(f"Total pages processed: {total_pages}")
        print(f"Successful: {successful_pages}")
        print(f"Failed: {failed_pages}")
        
        # Commit changes and cleanup
        conn.commit()
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        
if __name__ == "__main__":
    main()
