# /lib/tenant_connect/manager.py
import os
import threading
from typing import Dict, Optional

from lib.MongoDBDockerClient import MongoDBDockerClient
import logging

log_level = os.getenv('FD_LOG_LEVEL', 'INFO').upper()
logging.basicConfig(
    level=getattr(logging, log_level),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(os.getenv('FD_SERVICE_NAME', 'fd-simulate-reader'))


class TenantMongoManager:
    _thread_local = threading.local()
    _connections: Dict[str, any] = {}  # Dictionary to store all tenant connections

    @classmethod
    def initialize_connections(cls, connections: Dict[str, any]) -> None:
        """
        Initialize the connection pool with a dictionary of tenant connections.
        
        Args:
            connections: Dictionary mapping tenant_ids to their respective connections
        """
        cls._connections = connections

    @classmethod
    def set_active_connection(cls, tenant_id: str) -> None:
        """
        Set the active connection for the current thread based on tenant_id.
        
        Args:
            tenant_id: The ID of the tenant whose connection should be activated
        """
        if tenant_id not in cls._connections:
            raise ValueError(f"No connection found for tenant {tenant_id}")
        cls._thread_local.connection = cls._connections[tenant_id]

    @classmethod
    def get_active_connection(cls, collection_name: str) -> MongoDBDockerClient:
        """
        Get the active connection for the current thread.
        
        Returns:
            The active connection or None if no connection is set
        """
        connection = getattr(cls._thread_local, "connection", None)
        if connection is None:
            raise ValueError("No connection is set")
        return MongoDBDockerClient(
            current_db=connection.db,
            collection_name=collection_name
        )
    @classmethod
    def get_connection(cls, tenant_id: str, collection_name: str) -> MongoDBDockerClient:
        """
        Get a specific tenant's connection directly.
        
        Args:
            tenant_id: The ID of the tenant whose connection to retrieve
            
        Returns:
            The connection for the specified tenant or None if not found
        """
        connection = cls._connections.get(tenant_id)
        logger.info(f"Getting connection for tenant {tenant_id}")
        if connection is None:
            raise ValueError("No connection is set")
        return MongoDBDockerClient(
            current_db=connection.db,
            collection_name=collection_name
        )
