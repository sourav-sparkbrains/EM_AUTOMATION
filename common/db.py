import mysql.connector
import os
from mysql.connector import Error
from dotenv import load_dotenv
from urllib.parse import urlparse

load_dotenv()


def parse_railway_url(database_url):
    """
    Parse Railway database URL to extract connection parameters
    Example URL: mysql://user:password@host:port/database
    """
    parsed = urlparse(database_url)

    return {
        'host': parsed.hostname,
        'port': parsed.port or 3306,
        'user': parsed.username,
        'password': parsed.password,
        'database': parsed.path[1:]
    }


def create_connection():
    """Create database connection from Railway URL"""
    try:
        database_url = os.getenv("DATABASE_URL")

        if not database_url:
            raise Exception("DATABASE_URL not found in environment variables")

        print(f"Connecting to Railway database...")

        db_config = parse_railway_url(database_url)

        print(f"   Host: {db_config['host']}")
        print(f"   Port: {db_config['port']}")
        print(f"   Database: {db_config['database']}")

        connection = mysql.connector.connect(
            host=db_config['host'],
            port=db_config['port'],
            user=db_config['user'],
            password=db_config['password'],
            database=db_config['database']
        )

        if connection.is_connected():
            print("Successfully connected to Railway MySQL database")
            return connection

    except Error as e:
        print(f"Error connecting to MySQL: {e}")
        return None

my_db = create_connection()
cursor = my_db.cursor(dictionary=True)