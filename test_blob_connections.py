import os
from dotenv import load_dotenv

# Load environment variables from .env file in the project root
env_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path=env_path)

# Import the function to test
from app.lib.tenant_connect.load_connections import load_blob_connections

def test_load_blob_connections():
    try:
        load_blob_connections()
        print("load_blob_connections executed successfully.")
    except Exception as e:
        print(f"load_blob_connections failed: {e}")

if __name__ == "__main__":
    test_load_blob_connections() 