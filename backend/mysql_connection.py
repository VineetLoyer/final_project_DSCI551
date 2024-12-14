import mysql.connector
from mysql.connector import Error

def create_connection():
    """Create and return a connection to the MySQL database."""
    try:
        connection = mysql.connector.connect(
            host="localhost",
            user="root",  # Replace with your MySQL username
            password="NewPiece@0509",  # Replace with your MySQL password
            database="chatdb"
        )
        if connection.is_connected():
            print("MySQL Database connection successful")
            return connection
    except Error as e:
        print(f"Error connecting to MySQL: {e}")
        return None

def close_connection(connection):
    """Close the MySQL database connection."""
    if connection and connection.is_connected():
        connection.close()
        print("MySQL connection closed")
