from datetime import datetime
import psycopg2
import dotenv
import os

dotenv.load_dotenv()
db_name = "VectorDB01"
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")

class PostgresDB:
    def __init__(self, db_name, user, password, host='localhost', port='5432', schema='crawldb'):

        self.db_name = db_name
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.conn = None
        self.cursor = None
        self.schema = schema

    def connect(self):
        """Establishes a connection to the database."""
        try:
            self.conn = psycopg2.connect(
                dbname=self.db_name,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port
            )
            self.cursor = self.conn.cursor()
            print("Connected to:", self.db_name ,  self.get_version(), "at", self.host, "on port", self.port)
        except Exception as e:
            print("Connection error:", e)

    def _init_schema(self, schema_name):
        """Ensure crawldb schema and required columns exist"""
        if schema_name:
            self.schema = schema_name

        cursor = self.conn.cursor()
        try:
            cursor.execute("SELECT 1 FROM information_schema.schemata WHERE schema_name = '%s'", (self.schema,))
            if not cursor.fetchone():
                print("Creating crawldb schema...")
                try:
                    with open('init-scripts/crawldb.sql', 'r') as f:
                        schema_sql = f.read()
                        cursor.execute(schema_sql)
                    print(f"Schema {self.schema} created successfully")

                except Exception as e:
                    print(f"Error creating schema: {e}")
        
        except Exception as e:
            print(f"Error initializing schema: {e}")
        finally:
            cursor.close()

    def get_version(self):
        """Retrieves and returns the database version."""
        try:
            if self.cursor:
                self.cursor.execute("SELECT version();")
                return self.cursor.fetchone()[0]
        except Exception as e:
            print("Error fetching version:", e)
        return None

    def fetch_data(self, query, params=None):
        """Executes a SQL query and returns the result."""
        ""
        try:
            if self.cursor:
                self.cursor.execute(query, params)
                return self.cursor.fetchall()
        except Exception as e:
            print("Query execution error:", e)
        return None
    
    def insert_image(self, page_id, filename, content_type, image_data, accessed_time):
        try:
            query = """
            INSERT INTO crawldb.image (page_id, filename, content_type, "data", accessed_time)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id;
            """

            self.cursor.execute(query, (page_id, filename, content_type, image_data, accessed_time))
            image_id = self.cursor.fetchone()[0]
            self.conn.commit()

            print(f"Inserted image with ID {image_id} for page {page_id}.")
            return image_id
        except Exception as e:
            self.conn.rollback()
            print("Error inserting image:", e)

    def insert_data_type(self, code):
        try:
            self.cursor.execute("INSERT INTO crawldb.data_type (code) VALUES (%s) ON CONFLICT DO NOTHING;", (code,))
            self.conn.commit()
            print("Inserted into data_type:", code)
        except Exception as e:
            print("Error inserting data_type:", e)

    def insert_page_type(self, code):
        try:
            self.cursor.execute("INSERT INTO crawldb.page_type (code) VALUES (%s) ON CONFLICT DO NOTHING;", (code,))
            self.conn.commit()
            print("Inserted into page_type:", code)
        except Exception as e:
            print("Error inserting page_type:", e)

    def insert_site(self, domain, robots_content, sitemap_content):
        try:
            self.cursor.execute(
                "INSERT INTO crawldb.site (domain, robots_content, sitemap_content) VALUES (%s, %s, %s) RETURNING id;",
                (domain, robots_content, sitemap_content)
            )
            site_id = self.cursor.fetchone()[0]
            self.conn.commit()
            print("Inserted into site, ID:", site_id)
            return site_id
        except Exception as e:
            print("Error inserting site:", e)

    def insert_page_data(self, page_id, data_type_code, data):
        """Inserts data into the crawldb.page_data table."""
        try:
            self.cursor.execute(
                "INSERT INTO crawldb.page_data (page_id, data_type_code, data) VALUES (%s, %s, %s);",
                (page_id, data_type_code, data)
            )
            self.conn.commit()
            print(f"Inserted into page_data: page_id={page_id}, data_type_code={data_type_code}")
        except Exception as e:
            print("Error inserting page_data:", e)

    def insert_page(self, site_id, page_type_code, url, html_content, http_status_code, accessed_time):
        """Inserts a page into the crawldb.page table."""
        try:
            self.cursor.execute(
                """
                INSERT INTO crawldb.page (site_id, page_type_code, url, html_content, http_status_code, accessed_time)
                VALUES (%s, %s, %s, %s, %s, %s) RETURNING id;
                """,
                (site_id, page_type_code, url, html_content, http_status_code, accessed_time)
            )
            page_id = self.cursor.fetchone()[0]
            self.conn.commit()
            print(f"Inserted into page: ID={page_id}, URL={url}")
            return page_id
        except Exception as e:
            print("Error inserting page:", e)
            self.conn.rollback()
            return None  # Return None in case of failure
        
    def get_all_urls(self):
        """Fetches all URLs from the crawldb.page table and returns them as a set."""
        try:
            self.cursor.execute("SELECT url FROM crawldb.page;")
            urls = {row[0] for row in self.cursor.fetchall()}  # Fetch all and convert to a set
            return urls
        except Exception as e:
            print("Error fetching URLs:", e)
            return set()  # Return an empty set in case of failure
        
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
            
        except Exception as e:
            print(f"Error initializing schema: {e}")
        finally:
            cursor.close()

    def close(self):
        """Closes the database connection."""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        print("Database connection closed.")

    #SHIT FROM CRAWLER THAT MIGHT BE USEFULL 
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
        """Check if content hash exists in database, return first page with this hash (oldest)"""
        cursor = self.conn.cursor()
        try:
            cursor.execute("""
                SELECT id, url 
                FROM crawldb.page 
                WHERE content_hash = %s AND page_type_code = 'HTML'
                ORDER BY accessed_time ASC
                LIMIT 1
            """, (content_hash,))
            
            result = cursor.fetchone()
            if result:
                return result[0], result[1]  # id, url
            return None, None
        except Exception as e:
            print(f"Error checking content hash: {e}")
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

if __name__ == "__main__":
    db = PostgresDB(db_name, db_user, db_password, db_host, db_port)
    db.connect()
    db.close()
