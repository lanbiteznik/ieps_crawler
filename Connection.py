import psycopg2
import dotenv
import os

dotenv.load_dotenv()
db_name = os.getenv("DB_NAME")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")

class PostgresDB:
    def __init__(self, db_name, user, password, host='localhost', port='5432'):
        self.db_name = db_name
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.conn = None
        self.cursor = None

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
            print("Connected to:", self.get_version())
        except Exception as e:
            print("Connection error:", e)

    def get_version(self):
        """Retrieves and returns the database version."""
        try:
            if self.cursor:
                self.cursor.execute("SELECT version();")
                return self.cursor.fetchone()[0]
        except Exception as e:
            print("Error fetching version:", e)
        return None

    def execute_query(self, query, params=None):
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
                (page_id, data_type_code, psycopg2.Binary(data))
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
     
    def close(self):
        """Closes the database connection."""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        print("Database connection closed.")

if __name__ == "__main__":
    db = PostgresDB(db_name, db_user, db_password, db_host, db_port)
    db.connect()
    db.close()
