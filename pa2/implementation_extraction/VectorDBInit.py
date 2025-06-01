import psycopg2
from urllib.parse import urlparse
from datetime import datetime
import dotenv
import os

dotenv.load_dotenv(override=True)
db_name = os.getenv("DB_NAME")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")

conn = psycopg2.connect(
host=db_host,
port=db_port,
database=db_name,
user=db_user,
password=db_password,
)
conn.autocommit = True

from pgvector.psycopg2 import register_vector

cur = conn.cursor()

cur.execute('CREATE EXTENSION IF NOT EXISTS vector')

register_vector(conn)

conn.commit()
cur.close()
conn.close()