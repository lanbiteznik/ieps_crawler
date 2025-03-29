from Connection import PostgresDB
import os
import dotenv
dotenv.load_dotenv()
db_name = os.getenv("DB_NAME")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")

def analyze_data():
    db = PostgresDB(db_name, db_user, db_password, db_host, db_port)
    db.connect()

    # Number of sites
    query_sites = "SELECT COUNT(*) FROM crawldb.site;"
    num_sites = db.fetch_data(query_sites)[0][0]

    # Number of web pages
    query_pages = "SELECT COUNT(*) FROM crawldb.page;"
    num_pages = db.fetch_data(query_pages)[0][0]

    # Number of duplicates
    query_duplicates = "SELECT COUNT(*) FROM crawldb.page WHERE page_type_code = 'DUPLICATE';"
    num_duplicates = db.fetch_data(query_duplicates)[0][0]

    # Number of binary documents by type
    query_binaries = """
    SELECT data_type_code, COUNT(*) 
    FROM crawldb.page_data 
    GROUP BY data_type_code;
    """
    num_binaries = db.fetch_data(query_binaries)

    # Number of images
    query_images = "SELECT COUNT(*) FROM crawldb.image;"
    num_images = db.fetch_data(query_images)[0][0]

    # Average number of images per web page
    query_avg_images_per_page = """
    SELECT AVG(image_count) 
    FROM (
        SELECT COUNT(*) AS image_count
        FROM crawldb.image 
        GROUP BY page_id
    ) AS image_counts;
    """
    avg_images_per_page = db.fetch_data(query_avg_images_per_page)[0][0]

    # Print the results
    print(f"Number of Sites: {num_sites}")
    print(f"Number of Web Pages: {num_pages}")
    print(f"Number of Duplicates: {num_duplicates}")
    print(f"Number of Binary Documents by Type: {num_binaries}")
    print(f"Number of Images: {num_images}")
    print(f"Average Number of Images per Web Page: {avg_images_per_page:.2f}")

    db.close()

if __name__ == "__main__":
    analyze_data()