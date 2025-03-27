import psycopg2
from urllib.parse import urlparse
from datetime import datetime
import threading

class Database:
    # Add this as a class variable
    _processing_lock = threading.Lock()

    def __init__(self):
        try:
            # Connect to the existing wier database
            self.conn = psycopg2.connect(
                host="localhost",
                port=5432,
                database="wier",
                user="postgres",
                password="admin"
            )
            self.conn.autocommit = True
            
            # Ensure schema and tables are properly set up
            self._init_schema()
        except Exception as e:
            print(f"Database connection error: {e}")
            raise
    
    def _init_schema(self):
        """Ensure crawldb schema and required columns exist"""
        cursor = self.conn.cursor()
        try:
            # Check if crawldb schema exists
            cursor.execute("SELECT 1 FROM information_schema.schemata WHERE schema_name = 'crawldb'")
            if not cursor.fetchone():
                # Create schema from SQL file
                print("Creating crawldb schema...")
                try:
                    with open('init-scripts/crawldb.sql', 'r') as f:
                        schema_sql = f.read()
                        cursor.execute(schema_sql)
                    print("Schema created successfully")
                except Exception as e:
                    print(f"Error creating schema: {e}")
            
            # Add content_hash column if it doesn't exist
            cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_schema = 'crawldb' 
                AND table_name = 'page' 
                AND column_name = 'content_hash'
            """)
            if not cursor.fetchone():
                print("Adding content_hash column to page table...")
                cursor.execute("ALTER TABLE crawldb.page ADD COLUMN content_hash VARCHAR(32)")
        except Exception as e:
            print(f"Error initializing schema: {e}")
        finally:
            cursor.close()
    
    def __del__(self):
        if hasattr(self, 'conn') and self.conn:
            self.conn.close()
    
    def add_site(self, domain, robots_content=None, sitemap_content=None):
        """Add a new site to the database if it doesn't exist and return its ID"""
        cursor = self.conn.cursor()
        try:
            # Check if site already exists
            cursor.execute("SELECT id FROM crawldb.site WHERE domain = %s", (domain,))
            result = cursor.fetchone()
            
            if result:
                return result[0]
            
            # Insert new site
            cursor.execute(
                "INSERT INTO crawldb.site (domain, robots_content, sitemap_content) VALUES (%s, %s, %s) RETURNING id",
                (domain, robots_content, sitemap_content)
            )
            site_id = cursor.fetchone()[0]
            return site_id
        finally:
            cursor.close()
    
    def get_site_id(self, domain):
        cursor = self.conn.cursor()
        cursor.execute("SELECT id FROM crawldb.site WHERE domain = %s", (domain,))
        site_id = cursor.fetchone()
        cursor.close()
        return site_id[0] if site_id else None
    
    def add_page_to_frontier(self, url):
        """Add a URL to the frontier if it's not already in the database"""
        cursor = self.conn.cursor()
        try:
            # Check if page already exists
            cursor.execute("SELECT id FROM crawldb.page WHERE url = %s", (url,))
            if cursor.fetchone():
                return False
            
            # Get domain from URL
            parsed_url = urlparse(url)
            domain = f"{parsed_url.scheme}://{parsed_url.netloc}"
            
            # Get or create site
            site_id = self.add_site(domain)
            
            # Add to frontier
            cursor.execute(
                "INSERT INTO crawldb.page (site_id, url, page_type_code) VALUES (%s, %s, 'FRONTIER')",
                (site_id, url)
            )
            return True
        finally:
            cursor.close()
    
    def get_next_frontier_page(self):
        """Get the next URL from the frontier with improved duplicate checking"""
        cursor = self.conn.cursor()
        try:
            # CRITICAL: Get a URL that hasn't been marked as VISITED yet
            cursor.execute("""
                SELECT p.url 
                FROM crawldb.page p 
                WHERE p.page_type_code = 'FRONTIER' 
                AND p.url NOT IN (
                    SELECT url 
                    FROM crawldb.page 
                    WHERE page_type_code != 'FRONTIER'
                )
                ORDER BY p.id 
                LIMIT 1
            """)
            
            result = cursor.fetchone()
            if result:
                return result[0]
            return None
        finally:
            cursor.close()
    
    def get_next_frontier_page_preferential(self):
        """Get next page from frontier with preference for certain URL patterns"""
        cursor = self.conn.cursor()
        try:
            # Try any frontier page
            cursor.execute("""
                SELECT url FROM crawldb.page 
                WHERE page_type_code = 'FRONTIER'
                AND url NOT LIKE '%sitemap%.xml%'
                AND url NOT LIKE '%/assets/sitemap/%'
                LIMIT 1
            """)
            result = cursor.fetchone()
            # CRITICAL FIX: Handle None result safely
            return result[0] if result else None
        except Exception as e:
            print(f"Database error getting frontier page: {e}")
            return None
        finally:
            cursor.close()
    
    def update_page(self, url, html_content, http_status, page_type='HTML'):
        """Update a page after crawling"""
        cursor = self.conn.cursor()
        try:
            cursor.execute(
                """
                UPDATE crawldb.page 
                SET html_content = %s, http_status_code = %s, 
                    page_type_code = %s, accessed_time = %s
                WHERE url = %s
                RETURNING id
                """,
                (html_content, http_status, page_type, datetime.now(), url)
            )
            return cursor.fetchone()[0]
        finally:
            cursor.close()
    
    def update_page_with_hash(self, url, html_content, status_code, content_hash):
        """Update page with HTML content and hash"""
        cursor = self.conn.cursor()
        try:
            cursor.execute(
                """
                UPDATE crawldb.page 
                SET html_content = %s, http_status_code = %s, 
                    accessed_time = %s, page_type_code = 'HTML', content_hash = %s
                WHERE url = %s
                RETURNING id
                """,
                (html_content, status_code, datetime.now(), content_hash, url)
            )
            
            result = cursor.fetchone()
            if result:
                page_id = result[0]
                self.conn.commit()
                return page_id
            return None
        except Exception as e:
            print(f"Error updating page with hash: {e}")
            self.conn.rollback()
            return None
        finally:
            cursor.close()
    
    def add_link(self, from_url, to_url):
        """Add a link between two pages"""
        cursor = self.conn.cursor()
        try:
            # Get page IDs
            cursor.execute("SELECT id FROM crawldb.page WHERE url = %s", (from_url,))
            from_id = cursor.fetchone()
            if not from_id:
                return False
            
            cursor.execute("SELECT id FROM crawldb.page WHERE url = %s", (to_url,))
            to_id = cursor.fetchone()
            if not to_id:
                return False
            
            # Add link if it doesn't exist
            try:
                cursor.execute(
                    "INSERT INTO crawldb.link (from_page, to_page) VALUES (%s, %s)",
                    (from_id[0], to_id[0])
                )
                return True
            except psycopg2.IntegrityError:
                self.conn.rollback()  # Duplicate link
                return False
        finally:
            cursor.close()
    
    def add_image(self, page_id, filename, content_type, data=None):
        """Add an image to the database"""
        cursor = self.conn.cursor()
        try:
            cursor.execute(
                """
                INSERT INTO crawldb.image 
                (page_id, filename, content_type, data, accessed_time)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (page_id, filename, content_type, data, datetime.now())
            )
            return True
        finally:
            cursor.close()
    
    def add_binary_content(self, page_id, data_type_code, data):
        """Add binary content to page_data"""
        cursor = self.conn.cursor()
        try:
            print(f"  Adding binary content of type {data_type_code} for page {page_id}")
            
            # First check if data type exists in table
            cursor.execute("SELECT code FROM crawldb.data_type WHERE code = %s", (data_type_code,))
            if not cursor.fetchone():
                print(f"  Adding data_type: {data_type_code}")
                cursor.execute("INSERT INTO crawldb.data_type (code) VALUES (%s)", (data_type_code,))
            
            cursor.execute(
                "INSERT INTO crawldb.page_data (page_id, data_type_code, data) VALUES (%s, %s, %s)",
                (page_id, data_type_code, psycopg2.Binary(data))
            )
            self.conn.commit()
            return True
        except Exception as e:
            print(f"  Error adding binary content: {e}")
            self.conn.rollback()
            return False
        finally:
            cursor.close()
    
    def mark_as_duplicate(self, url, original_url):
        """Mark a page as duplicate of another page"""
        cursor = self.conn.cursor()
        try:
            cursor.execute(
                """
                UPDATE crawldb.page 
                SET page_type_code = 'DUPLICATE', accessed_time = %s, html_content = NULL
                WHERE url = %s
                """,
                (datetime.now(), url)
            )
            return True
        finally:
            cursor.close()
    
    def check_content_hash_exists(self, content_hash):
        """Check if page with same content hash exists"""
        cursor = self.conn.cursor()
        try:
            cursor.execute(
                "SELECT id, url FROM crawldb.page WHERE content_hash = %s LIMIT 1",
                (content_hash,)
            )
            result = cursor.fetchone()
            if result:
                return result[0], result[1]
            return None, None
        finally:
            cursor.close()
    
    def update_site_sitemap(self, domain, sitemap_content):
        """Update site with sitemap content"""
        cursor = self.conn.cursor()
        try:
            # First get site ID
            cursor.execute("SELECT id FROM crawldb.site WHERE domain = %s", (domain,))
            result = cursor.fetchone()
            
            if result:
                # Update existing site
                site_id = result[0]
                cursor.execute(
                    "UPDATE crawldb.site SET sitemap_content = %s WHERE id = %s",
                    (sitemap_content, site_id)
                )
                print(f"  Updated sitemap for existing site ID: {site_id}")
            else:
                # Create new site with sitemap
                cursor.execute(
                    "INSERT INTO crawldb.site (domain, sitemap_content) VALUES (%s, %s) RETURNING id",
                    (domain, sitemap_content)
                )
                site_id = cursor.fetchone()[0]
                print(f"  Created new site with sitemap, ID: {site_id}")
            
            # IMPORTANT: Explicitly commit the transaction
            self.conn.commit()
            
            # Verify the sitemap was saved
            cursor.execute(
                "SELECT LENGTH(sitemap_content) FROM crawldb.site WHERE id = %s", 
                (site_id,)
            )
            length = cursor.fetchone()[0]
            print(f"  Verified sitemap storage - Length: {length} bytes")
            
            return True
        except Exception as e:
            print(f"  Error updating site sitemap: {e}")
            self.conn.rollback()
            return False
        finally:
            cursor.close()
    
    def mark_page_as_processing(self, url):
        """Thread-safe tracking of processing pages"""
        cursor = self.conn.cursor()
        try:
            # Use lock for thread safety
            with Database._processing_lock:
                if not hasattr(self, 'processing_pages'):
                    self.processing_pages = set()
                self.processing_pages.add(url)
            
            # Update accessed_time
            cursor.execute(
                """
                UPDATE crawldb.page 
                SET accessed_time = NOW()
                WHERE url = %s
                """,
                (url,)
            )
            return True
        except Exception as e:
            print(f"Error marking page as processing: {e}")
            return False
        finally:
            cursor.close()
    
    def mark_page_as_processed(self, url, new_type='PROCESSED'):
        """Mark a page as processed even if it wasn't crawled"""
        cursor = self.conn.cursor()
        try:
            cursor.execute(
                """
                UPDATE crawldb.page 
                SET page_type_code = %s, accessed_time = NOW()
                WHERE url = %s AND page_type_code = 'FRONTIER'
                """,
                (new_type, url)
            )
            self.conn.commit()
            return True
        except Exception as e:
            print(f"Error marking page as processed: {e}")
            return False
        finally:
            cursor.close()
    
    def remove_sitemap_urls_from_frontier(self):
        """Remove sitemap files from frontier to prevent loops"""
        cursor = self.conn.cursor()
        try:
            cursor.execute("""
                DELETE FROM crawldb.page 
                WHERE page_type_code = 'FRONTIER'
                AND (
                    url LIKE '%sitemap%.xml%' OR
                    url LIKE '%/assets/sitemap/%'
                )
            """)
            deleted = cursor.rowcount
            self.conn.commit()
            print(f"Removed {deleted} sitemap URLs from frontier to prevent loops")
            return deleted
        except Exception as e:
            print(f"Error removing sitemap URLs: {e}")
            self.conn.rollback()
            return 0
        finally:
            cursor.close()
