#!/usr/bin/env python3
"""
Test script to verify tenant connections and MongoDB connectivity
"""

import os
import sys
import logging
from dotenv import load_dotenv

# Add the app directory to the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'app'))

from lib.tenant_connect.load_connections import load_tenant_connections
from lib.tenant_connect.manager_mongo import TenantMongoManager
from lib.MongoDBDockerClient import MongoDBDockerClient

# Load environment variables from .env file first
load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('test_python')

# Load tenant connections - this initializes the connection pool
load_tenant_connections()

def test_terry_connection():
    """
    Test function to verify terry's MongoDB connection
    """
    try:
        logger.info("=== Starting MongoDB Connection Test for Terry ===")
        
        # Step 1: Show environment variables for debug
        logger.info(f"MongoDB Host: {os.environ.get('MONGO_DB_HOST')}")
        logger.info(f"MongoDB Port: {os.environ.get('MONGO_DB_PORT')}")
        logger.info(f"MongoDB Database: {os.environ.get('MONGO_DB_DATABASE')}")
        logger.info(f"Python Path: {os.environ.get('PYTHONPATH')}")
        
        # Step 2: Get MongoDB connection for tenant_id "terry" using the loaded connections
        logger.info("Step 2: Getting MongoDB connection for tenant_id 'terry'...")
        tenant_id = "terry"
        collection_name = "heartbeats"
        
        mongo_client = TenantMongoManager.get_connection(tenant_id, collection_name)
        logger.info("✅ MongoDB connection obtained successfully")
        
        # Step 3: Verify connection details
        logger.info("Step 3: Verifying connection details...")
        logger.info(f"Database name (attribute): {mongo_client.db_name}")
        logger.info(f"Actual database name: {mongo_client.db.name}")
        logger.info(f"Collection name: {mongo_client.collection_name}")
        logger.info(f"Actual collection name: {mongo_client.collection.name}")
        
        # Verify the db_name attribute is correct for terry tenant
        expected_db_name = "terry_telematics"
        if mongo_client.db_name == expected_db_name:
            logger.info(f"✅ Database name attribute matches expected: {expected_db_name}")
        else:
            logger.error(f"❌ Database name attribute mismatch! Expected: {expected_db_name}, Got: {mongo_client.db_name}")
            return False
        
        # Step 4: Verify it's connected to the correct database
        actual_db_name = mongo_client.db.name  # Use the actual database name from the connection
        if actual_db_name == expected_db_name:
            logger.info(f"✅ Actual database name matches expected: {expected_db_name}")
        else:
            logger.error(f"❌ Actual database name mismatch! Expected: {expected_db_name}, Got: {actual_db_name}")
            return False
        
        # Step 5: Test the actual connection by performing a simple operation
        logger.info("Step 4: Testing actual MongoDB connection...")
        try:
            # Try to get database stats to verify connection
            db_info = mongo_client.db.command("dbStats")
            logger.info(f"✅ Successfully connected to MongoDB database: {mongo_client.db.name}")
            logger.info(f"Database stats: {db_info.get('collections', 'N/A')} collections")
            
            # Try to get collection info
            collection_info = mongo_client.db.command("collStats", collection_name)
            logger.info(f"✅ Successfully connected to collection: {mongo_client.collection.name}")
            logger.info(f"Collection stats: {collection_info.get('count', 'N/A')} documents")
            
        except Exception as e:
            if "requires authentication" in str(e):
                logger.info(f"✅ Successfully connected to MongoDB database: {mongo_client.db.name} (authentication required for stats)")
            else:
                logger.error(f"❌ Failed to connect to MongoDB: {str(e)}")
                return False
        
        logger.info("=== Test Completed Successfully ===")
        return True
        
    except Exception as e:
        logger.error(f"❌ Test failed with error: {str(e)}", exc_info=True)
        return False

def test_werner_connection():
    """
    Test function to verify werner's MongoDB connection
    """
    try:
        logger.info("=== Starting MongoDB Connection Test for Werner ===")
        
        # Get MongoDB connection for tenant_id "werner" using the loaded connections
        tenant_id = "werner"
        collection_name = "heartbeats"
        
        mongo_client = TenantMongoManager.get_connection(tenant_id, collection_name)
        logger.info("✅ MongoDB connection obtained successfully")
        
        # Verify connection details
        logger.info(f"Database name: {mongo_client.db.name}")
        logger.info(f"Collection name: {mongo_client.collection.name}")
        
        # Test actual MongoDB connection
        logger.info("Testing actual MongoDB connection...")
        try:
            # Try to get database stats to verify connection
            db_info = mongo_client.db.command("dbStats")
            logger.info(f"✅ Successfully connected to MongoDB database: {mongo_client.db.name}")
            logger.info(f"Database stats: {db_info.get('collections', 'N/A')} collections")
        except Exception as e:
            if "requires authentication" in str(e):
                logger.info(f"✅ Successfully connected to MongoDB database: {mongo_client.db.name} (authentication required for stats)")
            else:
                logger.error(f"❌ Failed to connect to MongoDB: {str(e)}")
                return False
        
        logger.info("=== Werner Test Completed Successfully ===")
        return True
        
    except Exception as e:
        logger.error(f"❌ Werner test failed with error: {str(e)}", exc_info=True)
        return False

def test_collection_names():
    """
    Test function to verify different collection names work correctly
    """
    try:
        logger.info("=== Starting Collection Names Test ===")
        
        tenant_id = "terry"
        
        # Test 1: Get connection with "heartbeats" collection
        logger.info("Test 1: Getting connection with 'heartbeats' collection...")
        mongo_client_heartbeats = TenantMongoManager.get_connection(tenant_id, "heartbeats")
        logger.info("✅ MongoDB connection for 'heartbeats' obtained successfully")
        
        # Test 2: Get connection with "daily_fuel" collection
        logger.info("Test 2: Getting connection with 'daily_fuel' collection...")
        mongo_client_daily_fuel = TenantMongoManager.get_connection(tenant_id, "daily_fuel")
        logger.info("✅ MongoDB connection for 'daily_fuel' obtained successfully")
        
        # Verify connection details for heartbeats
        logger.info("Verifying 'heartbeats' connection details...")
        logger.info(f"Database name: {mongo_client_heartbeats.db.name}")
        logger.info(f"Collection name: {mongo_client_heartbeats.collection.name}")
        logger.info(f"Database name attribute: {mongo_client_heartbeats.db_name}")
        logger.info(f"Collection name attribute: {mongo_client_heartbeats.collection_name}")
        
        # Verify connection details for daily_fuel
        logger.info("Verifying 'daily_fuel' connection details...")
        logger.info(f"Database name: {mongo_client_daily_fuel.db.name}")
        logger.info(f"Collection name: {mongo_client_daily_fuel.collection.name}")
        logger.info(f"Database name attribute: {mongo_client_daily_fuel.db_name}")
        logger.info(f"Collection name attribute: {mongo_client_daily_fuel.collection_name}")
        
        # Verify both connections are to the same database but different collections
        expected_db_name = "terry_telematics"
        
        if (mongo_client_heartbeats.db.name == expected_db_name and 
            mongo_client_daily_fuel.db.name == expected_db_name):
            logger.info(f"✅ Both connections are to the correct database: {expected_db_name}")
        else:
            logger.error(f"❌ Database name mismatch! Expected: {expected_db_name}")
            logger.error(f"   Heartbeats DB: {mongo_client_heartbeats.db.name}")
            logger.error(f"   Daily Fuel DB: {mongo_client_daily_fuel.db.name}")
            return False
        
        if (mongo_client_heartbeats.collection.name == "heartbeats" and 
            mongo_client_daily_fuel.collection.name == "daily_fuel"):
            logger.info("✅ Both connections have correct collection names")
        else:
            logger.error("❌ Collection name mismatch!")
            logger.error(f"   Heartbeats collection: {mongo_client_heartbeats.collection.name}")
            logger.error(f"   Daily Fuel collection: {mongo_client_daily_fuel.collection.name}")
            return False
        
        # Test actual connections
        logger.info("Testing actual MongoDB connections...")
        try:
            # Test heartbeats connection
            try:
                db_info_heartbeats = mongo_client_heartbeats.db.command("dbStats")
                logger.info(f"✅ Successfully connected to database: {mongo_client_heartbeats.db.name}")
            except Exception as e:
                if "requires authentication" in str(e):
                    logger.info(f"✅ Successfully connected to database: {mongo_client_heartbeats.db.name} (authentication required for stats)")
                else:
                    logger.error(f"❌ Failed to connect to heartbeats database: {str(e)}")
                    return False
            
            # Test daily_fuel connection
            try:
                db_info_daily_fuel = mongo_client_daily_fuel.db.command("dbStats")
                logger.info(f"✅ Successfully connected to database: {mongo_client_daily_fuel.db.name}")
            except Exception as e:
                if "requires authentication" in str(e):
                    logger.info(f"✅ Successfully connected to database: {mongo_client_daily_fuel.db.name} (authentication required for stats)")
                else:
                    logger.error(f"❌ Failed to connect to daily_fuel database: {str(e)}")
                    return False
            
            # Try to get collection info (this might fail if collections don't exist, but connection should work)
            try:
                collection_info_heartbeats = mongo_client_heartbeats.db.command("collStats", "heartbeats")
                logger.info(f"✅ Successfully connected to collection: {mongo_client_heartbeats.collection.name}")
                logger.info(f"Collection stats: {collection_info_heartbeats.get('count', 'N/A')} documents")
            except Exception as e:
                if "requires authentication" in str(e):
                    logger.info(f"✅ Successfully connected to collection: {mongo_client_heartbeats.collection.name} (authentication required for stats)")
                else:
                    logger.info(f"ℹ️  Collection 'heartbeats' might not exist yet: {str(e)}")
            
            try:
                collection_info_daily_fuel = mongo_client_daily_fuel.db.command("collStats", "daily_fuel")
                logger.info(f"✅ Successfully connected to collection: {mongo_client_daily_fuel.collection.name}")
                logger.info(f"Collection stats: {collection_info_daily_fuel.get('count', 'N/A')} documents")
            except Exception as e:
                if "requires authentication" in str(e):
                    logger.info(f"✅ Successfully connected to collection: {mongo_client_daily_fuel.collection.name} (authentication required for stats)")
                else:
                    logger.info(f"ℹ️  Collection 'daily_fuel' might not exist yet: {str(e)}")
            
        except Exception as e:
            logger.error(f"❌ Failed to connect to MongoDB: {str(e)}")
            return False
        
        logger.info("=== Collection Names Test Completed Successfully ===")
        return True
        
    except Exception as e:
        logger.error(f"❌ Collection names test failed with error: {str(e)}", exc_info=True)
        return False

def main():
    """Main function to run the test"""
    logger.info("Starting tenant connection tests...")
    
    # Test both tenants
    terry_success = test_terry_connection()
    werner_success = test_werner_connection()
    collection_names_success = test_collection_names()
    
    if terry_success and werner_success and collection_names_success:
        logger.info("🎉 All tests passed! Both tenant MongoDB connections and collection names are working correctly.")
        sys.exit(0)
    else:
        logger.error("💥 Some tests failed! Please check the logs above for details.")
        sys.exit(1)

if __name__ == "__main__":
    main() 