import psycopg2
from urllib.parse import urlparse
from datetime import datetime
import threading

import dotenv
import os

dotenv.load_dotenv()
db_name = os.getenv("DB_NAME")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")

class Database:
    # Add this as a class variable
    _processing_lock = threading.Lock()

    def __init__(self):
        try:
            # Connect to the existing wier database
            self.conn = psycopg2.connect(
                host=db_host,
                port=db_port,
                database=db_name,
                user=db_user,
                password=db_password,
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
            
            cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_schema = 'crawldb' 
                AND table_name = 'page' 
                AND column_name = 'content_minhash'
            """)
            if not cursor.fetchone():
                print("Adding content_minhash column to page table...")
                cursor.execute("ALTER TABLE crawldb.page ADD COLUMN content_minhash VARCHAR(512)")

            # Add duplicate_id column if it doesn't exist
            cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_schema = 'crawldb' 
                AND table_name = 'page' 
                AND column_name = 'duplicate_id'
            """)
            if not cursor.fetchone():
                print("Adding duplicate_id column to page table...")
                cursor.execute("""
                    ALTER TABLE crawldb.page ADD COLUMN IF NOT EXISTS duplicate_id integer;
                    ALTER TABLE crawldb.page ADD CONSTRAINT fk_duplicate_page FOREIGN KEY (duplicate_id) 
                        REFERENCES crawldb.page(id);
                """)
        except Exception as e:
            print(f"Error initializing schema: {e}")
        finally:
            cursor.close()
    
    def __del__(self):
        if hasattr(self, 'conn') and self.conn:
            self.conn.close()
    
    def add_site(self, domain, robots_content=None, sitemap_content=None):
        """Add a new site to the database if it doesn't exist and return its ID"""
        domain = self.validate_and_clean_domain(domain)
        if not domain:
            return None
            
        cursor = self.conn.cursor()
        try:
            # Check if site already exists
            cursor.execute("SELECT id FROM crawldb.site WHERE domain = %s", (domain,))
            result = cursor.fetchone()
            
            if result:
                site_id = result[0]
                
                # Update robots content if provided and different
                if robots_content:
                    cursor.execute("""
                        UPDATE crawldb.site 
                        SET robots_content = %s 
                        WHERE id = %s AND (robots_content IS NULL OR robots_content != %s)
                    """, (robots_content, site_id, robots_content))
                    
                self.conn.commit()
                return site_id
            else:
                # Create new site
                cursor.execute(
                    "INSERT INTO crawldb.site (domain, robots_content) VALUES (%s, %s) RETURNING id",
                    (domain, robots_content)
                )
                site_id = cursor.fetchone()[0]
                self.conn.commit()
                return site_id
        finally:
            cursor.close()
    
    def get_site_id(self, domain):
        cursor = self.conn.cursor()
        cursor.execute("SELECT id FROM crawldb.site WHERE domain = %s", (domain,))
        site_id = cursor.fetchone()
        cursor.close()
        return site_id[0] if site_id else None
    
    def add_page_to_frontier(self, url, priority=0):
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
            if site_id is None:  # This can happen if domain validation fails
                print(f"⚠️ Could not add site for URL: {url}")
                return False
            
            # Add to frontier
            cursor.execute(
                "INSERT INTO crawldb.page (site_id, url, page_type_code, priority) VALUES (%s, %s, 'FRONTIER', %s)",
                (site_id, url, priority)
            )
            self.conn.commit()  # Explicit commit needed even with autocommit
            return True
        except Exception as e:
            print(f"Error adding page to frontier: {e}")
            self.conn.rollback()
            return False
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
        """Get a page from frontier with preference for URLs containing keywords"""
        cursor = self.conn.cursor()
        try:
            # First check for preferential keywords
            if hasattr(self, 'preferential_keywords') and self.preferential_keywords:
                # Build dynamic query for preferential URLs
                conditions = []
                params = []
                
                for keyword in self.preferential_keywords:
                    conditions.append("url ILIKE %s")
                    params.append(f'%{keyword}%')
                    
                where_clause = " OR ".join(conditions)
                
                query = f"""
                    SELECT url FROM crawldb.page 
                    WHERE page_type_code = 'FRONTIER'
                    AND ({where_clause})
                    ORDER BY priority ASC, id ASC
                    LIMIT 1
                """
                
                cursor.execute(query, params)
                result = cursor.fetchone()
                
                if result:
                    print(f"Selected preferential URL: {result[0]}")
                    return result[0]
                    
                # If no preferential match, log it
                print("No preferential URLs found in frontier, using regular selection")
            
            # Fall back to any frontier URL
            cursor.execute("""
                SELECT url FROM crawldb.page 
                WHERE page_type_code = 'FRONTIER'
                ORDER BY priority ASC, id ASC
                LIMIT 1
            """)
            result = cursor.fetchone()
            
            if result:
                return result[0]
            return None
        except Exception as e:
            print(f"Error in preferential page selection: {e}")
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
    
    def update_page_with_hash_and_minhash(self, url, html_content, status_code, content_hash, content_minhash=None):
        """Update page with HTML content and hash - respecting existing duplicate marking"""
        cursor = self.conn.cursor()
        try:
            # First check if page exists and if it's already a DUPLICATE
            cursor.execute(
                "SELECT id, page_type_code FROM crawldb.page WHERE url = %s", 
                (url,)
            )
            result = cursor.fetchone()
            
            if result:
                page_id, page_type = result
                
                # If page is already marked as a duplicate, don't change its status
                if page_type == 'DUPLICATE':
                    print(f"Skipping content update for {url} - already marked as DUPLICATE")
                    # Only update the content hash without changing the page type
                    cursor.execute(
                        """
                        UPDATE crawldb.page 
                        SET content_hash = %s
                        WHERE id = %s
                        """,
                        (content_hash, page_id)
                    )
                    self.conn.commit()
                    return page_id
                    
                # Otherwise, update as normal (FIXED: removed trailing comma)
                cursor.execute(
                    """
                    UPDATE crawldb.page 
                    SET html_content = %s, 
                        http_status_code = %s,
                        page_type_code = 'HTML',
                        content_hash = %s, 
                        content_minhash = %s
                    WHERE id = %s
                    """,
                    (html_content, status_code, content_hash, content_minhash, page_id)
                )
            else:
                # Get domain and site_id
                parsed_url = urlparse(url)
                domain = f"{parsed_url.scheme}://{parsed_url.netloc}"
                site_id = self.add_site(domain)
                
                # Fixed: Fixed column/value count mismatch
                cursor.execute(
                    """
                    INSERT INTO crawldb.page (site_id, url, html_content, http_status_code, page_type_code, content_hash, content_minhash) 
                    VALUES (%s, %s, %s, %s, 'HTML', %s, %s) RETURNING id
                    """,
                    (site_id, url, html_content, status_code, content_hash, content_minhash)
                )
                page_id = cursor.fetchone()[0]
            
            self.conn.commit()
            return page_id
        except Exception as e:
            print(f"Error updating page: {e}")
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
    
    def mark_as_duplicate(self, duplicate_url, original_url):
        """Mark a page as a duplicate of another page"""
        cursor = self.conn.cursor()
        try:
            # Get the ID of the original page
            cursor.execute("SELECT id FROM crawldb.page WHERE url = %s", (original_url,))
            original_id = cursor.fetchone()
            if not original_id:
                print(f"Original URL not found: {original_url}")
                return False
            original_id = original_id[0]
            
            # Update the duplicate page - set page_type_code AND duplicate_id
            cursor.execute(
                """
                UPDATE crawldb.page 
                SET page_type_code = 'DUPLICATE', 
                    html_content = NULL,
                    duplicate_id = %s
                WHERE url = %s
                """,
                (original_id, duplicate_url)
            )
            self.conn.commit()
            return True
        except Exception as e:
            print(f"Error marking page as duplicate: {e}")
            return False
        finally:
            cursor.close()
    
    def check_content_hash_exists(self, content_hash):
        """Check if content hash exists in database"""
        cursor = self.conn.cursor()
        try:
            cursor.execute("""
                SELECT id, url 
                FROM crawldb.page 
                WHERE content_hash = %s 
                AND content_hash IS NOT NULL
                AND id != (SELECT MAX(id) FROM crawldb.page WHERE content_hash = %s)
                ORDER BY id ASC
                LIMIT 1
            """, (content_hash, content_hash))
            
            result = cursor.fetchone()
            if result:
                print(f"Found HASH duplicate: {result[1]}")
                return result[0], result[1]  # id, url
            return None, None
        finally:
            cursor.close()

    def get_duplicate_page_by_minhash(self, existing_minhash):
        """Get the ID of a duplicate page based on minhash"""
        cursor = self.conn.cursor()
        try:
            cursor.execute("""
                SELECT id, url 
                FROM crawldb.page 
                WHERE content_minhash = %s AND page_type_code = 'HTML'
                ORDER BY accessed_time ASC
                LIMIT 1
            """, (existing_minhash,))
            
            result = cursor.fetchone()
            if result:
                return result[0], result[1]  # id, url
            return None, None
        except Exception as e:
            print(f"Error checking minhash: {e}")
            return None, None
        finally:
            cursor.close()
    def get_duplicate_page_by_minhash_hex(self, minhash_hex):
        """Check for duplicate based on minhash"""
        cursor = self.conn.cursor()
        try:
            cursor.execute("""
                SELECT id, url 
                FROM crawldb.page 
                WHERE content_minhash = %s
                AND content_minhash IS NOT NULL
                AND id != (SELECT MAX(id) FROM crawldb.page WHERE content_minhash = %s)
                ORDER BY id ASC
                LIMIT 1
            """, (minhash_hex, minhash_hex))
            
            result = cursor.fetchone()
            if result:
                print(f"Found MINHASH duplicate: {result[1]}")
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
    
    def mark_page_as_processed(self, url, new_type='HTML'):
        """Mark a page as processed with a valid page type"""
        cursor = self.conn.cursor()
        try:
            # Get valid page types
            cursor.execute("SELECT code FROM crawldb.page_type")
            valid_types = [row[0] for row in cursor.fetchall()]
            
            # If the requested type is not valid, default to HTML
            if new_type not in valid_types:
                print(f"Warning: '{new_type}' is not a valid page type. Using 'HTML' instead.")
                new_type = 'HTML'  # Should be a valid type in most schemas
                
            cursor.execute(
                """
                UPDATE crawldb.page 
                SET page_type_code = %s, accessed_time = NOW()
                WHERE url = %s
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

    def validate_and_clean_domain(self, domain):
        """Clean up and validate domain format before storing"""
        # Handle empty domain
        if not domain:
            return None
            
        # Remove any protocol prefix if present
        if domain.startswith(("http://", "https://")):
            parsed = urlparse(domain)
            # Keep the scheme + netloc, not just netloc
            domain = f"{parsed.scheme}://{parsed.netloc}"
        
        # Check for invalid domains
        if not domain or '@' in domain or domain == '://' or len(domain) < 3:
            print(f"Skipping invalid domain: {domain}")
            return None
        
        return domain

    def cleanup_invalid_sites(self):
        """Remove invalid site entries like email addresses from the database"""
        cursor = self.conn.cursor()
        try:
            print("Cleaning up invalid sites...")
            # Find sites with @ in domain (email addresses)
            cursor.execute("SELECT id, domain FROM crawldb.site WHERE domain LIKE '%@%'")
            email_sites = cursor.fetchall()
            
            # Find completely invalid domains
            cursor.execute("SELECT id, domain FROM crawldb.site WHERE domain = '://' OR LENGTH(domain) < 3")
            invalid_sites = cursor.fetchall()
            
            all_invalid = email_sites + invalid_sites
            
            if all_invalid:
                print(f"Found {len(all_invalid)} invalid site entries:")
                for site_id, domain in all_invalid:
                    print(f"  - ID: {site_id}, Invalid domain: {domain}")
                    
                print("These entries cannot be accessed via browser and should be removed.")
            else:
                print("No invalid site entries found.")
                
            return len(all_invalid)
        finally:
            cursor.close()

    def set_preferential_keywords(self, keywords):
        """Set keywords for preferential crawling"""
        if keywords and isinstance(keywords, list):
            self.preferential_keywords = keywords
            print(f"Set preferential crawling keywords: {self.preferential_keywords}")
            return True
        return False

    def clean_test_urls(self, urls):
        """Clean up test URLs in a way that respects foreign key constraints"""
        cursor = self.conn.cursor()
        try:
            # First get the page IDs 
            page_ids = []
            for url in urls:
                cursor.execute("SELECT id FROM crawldb.page WHERE url = %s", (url,))
                result = cursor.fetchone()
                if result:
                    page_ids.append(result[0])
            
            if page_ids:
                # Delete links first
                for page_id in page_ids:
                    cursor.execute(
                        "DELETE FROM crawldb.link WHERE from_page = %s OR to_page = %s", 
                        (page_id, page_id)
                    )
                
                # Now it's safe to delete the pages
                for url in urls:
                    cursor.execute("DELETE FROM crawldb.page WHERE url = %s", (url,))
            
            self.conn.commit()
            return True
        except Exception as e:
            print(f"Error cleaning test data: {e}")
            return False
        finally:
            cursor.close()

    def verify_frontier_urls(self, expected_urls):
        """Debug method: verify that URLs are actually in the frontier"""
        cursor = self.conn.cursor()
        try:
            # Check how many URLs are in the frontier
            cursor.execute("SELECT COUNT(*) FROM crawldb.page WHERE page_type_code = 'FRONTIER'")
            count = cursor.fetchone()[0]
            print(f"Total frontier URLs: {count}")
            
            # Check how many of our expected URLs are in the frontier
            placeholders = ','.join(['%s'] * len(expected_urls))
            cursor.execute(f"SELECT url FROM crawldb.page WHERE page_type_code = 'FRONTIER' AND url IN ({placeholders})", 
                         tuple(expected_urls))
            found_urls = [row[0] for row in cursor.fetchall()]
            print(f"Found {len(found_urls)} of {len(expected_urls)} expected URLs in the frontier")
            
            # Show which URLs are missing
            missing = set(expected_urls) - set(found_urls)
            if missing:
                print("Missing URLs:")
                for url in missing:
                    print(f"  - {url}")
                    
            return len(found_urls)
        finally:
            cursor.close()

    def get_frontier_batch(self, limit=1000):
        """Get a batch of URLs from the frontier"""
        cursor = self.conn.cursor()
        try:
            cursor.execute("""
                SELECT url, priority FROM crawldb.page 
                WHERE page_type_code = 'FRONTIER'
                ORDER BY priority ASC, id ASC
                LIMIT %s
            """, (limit,))
            
            return [(row[0], row[1] or 0) for row in cursor.fetchall()]
        except Exception as e:
            print(f"Error retrieving frontier batch: {e}")
            return []
        finally:
            cursor.close()