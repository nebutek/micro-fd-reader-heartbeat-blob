#!/usr/bin/env python3
"""
Fleet Device Reader Service
This service consumes telemetry data from Kafka using the KafkaReader class.
"""

import os
import logging
import sys
import time
from typing import Dict, Any

import arrow
from lib.tenant_connect.load_connections import load_blob_connections
from lib.kafka_reader import KafkaReader
from lib.hos_event_handler import HOSEvent
from lib.MongoDBDockerClient import MongoDBDockerClient
from lib.BlobWriter import BlobWriter
from lib.CosmosDBManager import CosmosDBManager
from lib.tenant_connect.manager_blob import TenantBlobManager
from blob_data_handler import initialize_blob_writers, dispatch_blob_message

# Setup logging
log_level = os.getenv('FD_LOG_LEVEL', 'INFO').upper()
logging.basicConfig(
    level=getattr(logging, log_level),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Get configuration from environment
SERVICE_NAME = os.getenv('FD_SERVICE_NAME', 'fd-simulate-reader')
KAFKA_TOPIC = os.getenv('FD_KAFKA_TOPIC', 'telematics.raw.telematics_heartbeat')
MONGO_COLLECTION_NAME = os.getenv('FD_MONGO_COLLECTION_NAME', 'heartbeats')

# Initialize logger
logger = logging.getLogger(SERVICE_NAME)

# Initialize tenant connections
load_blob_connections()
# Initialize blob writers
initialize_blob_writers()


def process_message(message):
    """
    Process a message from Kafka
    
    Args:
        message: The message payload (already deserialized)
        
    Returns:
        bool: True if processing succeeded, False otherwise
    """
    try:
        # Validate required fields
        required_fields = ['tenant_id', 'asset_id', 'timestamp']
        for field in required_fields:
            if field not in message:
                raise ValueError(f"Missing required field: {field}")
        
        # Log message processing
        logger.debug(f"Processing message: tenant_id: {message.get('tenant_id')}, asset_id: {message.get('asset_id')}, driver_id: {message.get('driver_id')}, driver_name: {message.get('driver_name')}, timestamp: {message.get('timestamp')}")
        
        # Convert timestamp to datetime object
        try:
            timestamp = arrow.get(message.get('timestamp'))
            message['timestamp'] = timestamp.datetime
        except (ValueError, TypeError) as e:
            logger.error(f"Invalid timestamp format: {message.get('timestamp')} - {str(e)}")
            return False

        tenant_id = message.get('tenant_id')
        # Dispatch message to the correct BlobWriter
        dispatch_blob_message(tenant_id, message.get('data'))
        return True
        
    except ValueError as e:
        logger.error(f"Data validation error: {str(e)}")
        return False
    except KeyError as e:
        logger.error(f"Missing required field: {str(e)}")
        return False
    except (ConnectionError, TimeoutError) as e:
        logger.error(f"Database connection error: {str(e)}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error processing message: {str(e)}", exc_info=True)
        return False

def main():
    """Main entry point for the service"""
    logger.info(f"Starting {SERVICE_NAME} service")
        
    retry_count = 0
    max_retries = int(os.getenv('FD_MAX_RETRIES', '5'))
    retry_delay = int(os.getenv('FD_RETRY_DELAY', '30'))  # seconds
    reader = None
    
    while True:
        try:
            # Only create a new reader if we don't have one
            if reader is None:
                reader = KafkaReader(
                    callback=process_message,
                    service_name=SERVICE_NAME,
                    kafka_topic=KAFKA_TOPIC,
                    auto_offset_reset='earliest'  # For testing - read from earliest messages
                )
            
            # Run forever (this will block)
            reader.run_forever()
        except KeyboardInterrupt:
            logger.info("Service shutting down due to interrupt")
            break
        except (ConnectionError, TimeoutError) as e:
            logger.error(f"Network connection error in main loop: {str(e)}")
            retry_count += 1
        except ValueError as e:
            logger.error(f"Configuration error in main loop: {str(e)}")
            retry_count += 1
        except Exception as e:
            logger.error(f"Unexpected error in main loop: {str(e)}", exc_info=True)
            retry_count += 1
            
        if retry_count > max_retries:
            logger.error(f"Maximum retries ({max_retries}) exceeded. Entering failsafe mode.")
            # Failsafe loop to keep the pod running even if there's an error
            while True:
                try:
                    time.sleep(300)  # 5 minutes between retries
                    logger.info("Service is in failsafe mode")
                except KeyboardInterrupt:
                    logger.info("Service shutting down due to interrupt")
                    return
                except Exception:
                    pass  # Catch all exceptions to ensure the loop continues
            
        logger.info(f"Retrying in {retry_delay} seconds (attempt {retry_count}/{max_retries})")
        time.sleep(retry_delay)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Service shutting down due to interrupt")
    except Exception as e:
        logger.error(f"Critical error in application: {str(e)}", exc_info=True)
        # Keep container running for debugging
        while True:
            try:
                time.sleep(3600)
            except KeyboardInterrupt:
                break

