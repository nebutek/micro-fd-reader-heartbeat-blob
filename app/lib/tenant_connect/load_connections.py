import os
import logging
from typing import Dict, List

from lib.BlobWriter import BlobWriter
from lib import MongoDBDockerClient
from lib.tenant_connect.manager_blob import TenantBlobManager
from ..CosmosDBManager import CosmosDBManager
from .manager import TenantConnectManager
from .manager_mongo import TenantMongoManager
from ..MongoDBDockerClient import MongoDBDockerClient

# Setup logging
log_level = os.getenv('FD_LOG_LEVEL', 'INFO').upper()
logging.basicConfig(
    level=getattr(logging, log_level),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(os.getenv('FD_SERVICE_NAME', 'fd-simulate-reader'))

def load_tenant_connections() -> None:
    """
    Load and initialize CosmosDB connections for multiple tenants.
    This function defines the connection configurations internally and initializes them.
    """
    # Define the connection configurations
    connections: Dict[str, CosmosDBManager] = {
        "werner": CosmosDBManager(
            database_id="WERNER",
            container_name="alerts"
        ),
        "dev": CosmosDBManager(
            database_id="DEV",
            container_name="alerts",
        ),
        "terry": CosmosDBManager(
            database_id="DEFAULT",
            container_name="alerts"
        ),
        "admin": CosmosDBManager(
            database_id="DEFAULT",
            container_name="admin"
        ),
    }

    # Initialize the connections in the TenantConnectManager
    TenantConnectManager.initialize_connections(connections)

    # Initialize the connections in the TenantMongoManager
    mongo_connections: Dict[str, MongoDBDockerClient] = { 
        "werner": MongoDBDockerClient(
            db_name="telematics",
            collection_name="heartbeats"
        ),
        "terry": MongoDBDockerClient(
            db_name="terry_telematics",
            collection_name="heartbeats"
        )
    }
    TenantMongoManager.initialize_connections(mongo_connections)

def _get_tenants() -> List[str]:
    cosmos_db_manager = CosmosDBManager(
        database_id="DEFAULT",
        container_name="admin"
    )
    # pass parameters to the query
       
    tenants = cosmos_db_manager.query("SELECT * FROM c WHERE c.fd_type = 'tenant'")
    return tenants

def load_blob_connections() -> None:
    blob_connections: Dict[str, BlobWriter] = {}
    for tenant in _get_tenants():
        # Check for blob container in the tenant's connection
        blob_info = tenant.get("connection", {}).get("blob", {})
        blob_container = blob_info.get("container")
        if blob_container:
            logger.info(f"Blob container: {tenant['tenant_id']} {blob_container}")
            # Optionally, get a connection string if present, else rely on env
            blob_connections[tenant["tenant_id"]] = BlobWriter(
                container_name=blob_container
            )
    TenantBlobManager.initialize_connections(blob_connections)