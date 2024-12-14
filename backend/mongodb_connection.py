from pymongo import MongoClient

def create_connection():
    """Create and return a connection to the MongoDB database."""
    try:
        # Connect to the MongoDB server
        client = MongoClient("mongodb://localhost:27017/")
        db = client["chatdb"]
        print("MongoDB connection successful")
        return client,db
    except Exception as e:
        print(f"Error connecting to MongoDB: {e}")
        return None

def close_connection(client):
    """Close the MongoDB connection."""
    if client is not None:
        client.close()
        print("MongoDB connection closed")
