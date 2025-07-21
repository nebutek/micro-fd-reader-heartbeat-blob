# /lib/tenant_connect/manager.py
import os
import threading
from typing import Dict, Optional

from lib.BlobWriter import BlobWriter
import logging

log_level = os.getenv('FD_LOG_LEVEL', 'INFO').upper()
logging.basicConfig(
    level=getattr(logging, log_level),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(os.getenv('FD_SERVICE_NAME', 'fd-simulate-reader'))


class TenantBlobManager:
    _thread_local = threading.local()
    _connections: Dict[str, BlobWriter] = {}  # Dictionary to store all tenant connections

    @classmethod
    def initialize_connections(cls, connections: Dict[str, BlobWriter]) -> None:
        """
        Initialize the connection pool with a dictionary of tenant connections.
        
        Args:
            connections: Dictionary mapping tenant_ids to their respective connections
        """
        cls._connections = connections

    @classmethod
    def get_connection(cls, tenant_id: str) -> BlobWriter:
        """
        Get a specific tenant's BlobWriter connection directly.
        Args:
            tenant_id: The ID of the tenant whose connection to retrieve
        Returns:
            The BlobWriter connection for the specified tenant or raises if not found
        """
        connection = cls._connections.get(tenant_id)
        logger.info(f"Getting connection for tenant {tenant_id}")
        if connection is None:
            raise ValueError("No connection is set")
        return connection
