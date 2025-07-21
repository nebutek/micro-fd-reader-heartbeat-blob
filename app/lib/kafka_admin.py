#!/usr/bin/env python3
"""
Kafka admin utility functions for Fleet Defender services
"""

import os
import logging
from kafka.admin import KafkaAdminClient, NewPartitions, NewTopic
import json

# Setup logging
logger = logging.getLogger('fd-kafka-admin')

def change_topic_partitions(topic_name, new_partition_count, bootstrap_servers=None, security_protocol="PLAINTEXT"):
    """
    Change the number of partitions for a Kafka topic
    
    Args:
        topic_name (str): The name of the topic to modify
        new_partition_count (int): The desired number of partitions (must be greater than current count)
        bootstrap_servers (str): Kafka broker(s) addresses (comma-separated list)
        security_protocol (str): Security protocol to use (default: "PLAINTEXT")
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Use environment variable if bootstrap_servers not provided
        if bootstrap_servers is None:
            bootstrap_servers = os.getenv('FD_KAFKA_BROKER', os.getenv('KAFKA_BROKER'))
            if not bootstrap_servers:
                logger.error("No Kafka bootstrap servers provided")
                return False
        
        logger.info(f"Changing partition count for topic '{topic_name}' to {new_partition_count}")
        
        # Create admin client
        admin_client = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            security_protocol=security_protocol
        )
        
        # Get current topic info to verify partition count
        topic_info = admin_client.describe_topics([topic_name])
        current_partitions = len(topic_info[0]['partitions'])
        
        if new_partition_count <= current_partitions:
            logger.error(f"New partition count ({new_partition_count}) must be greater than current count ({current_partitions})")
            return False
            
        # Create the new partitions configuration
        topic_partitions = {
            topic_name: NewPartitions(total_count=new_partition_count)
        }
        
        # Create new partitions
        admin_client.create_partitions(topic_partitions)
        logger.info(f"Successfully changed partition count for topic '{topic_name}' from {current_partitions} to {new_partition_count}")
        
        # Close the admin client
        admin_client.close()
        return True
        
    except Exception as e:
        logger.error(f"Failed to change partition count: {str(e)}")
        return False

def create_topic(topic_name, num_partitions=1, replication_factor=1, config={}, bootstrap_servers=None, security_protocol="PLAINTEXT"):
    """
    Create a new Kafka topic
    
    Args:
        topic_name (str): The name of the topic to create
        num_partitions (int): Number of partitions (default: 1)
        replication_factor (int): Replication factor (default: 1)
        config (dict): Additional topic configuration parameters
        bootstrap_servers (str): Kafka broker(s) addresses (comma-separated list)
        security_protocol (str): Security protocol to use (default: "PLAINTEXT")
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Use environment variable if bootstrap_servers not provided
        if bootstrap_servers is None:
            bootstrap_servers = os.getenv('FD_KAFKA_BROKER', os.getenv('KAFKA_BROKER'))
            if not bootstrap_servers:
                logger.error("No Kafka bootstrap servers provided")
                return False
                
        logger.info(f"Creating topic '{topic_name}' with {num_partitions} partitions and replication factor {replication_factor}")
        
        # Create admin client
        admin_client = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            security_protocol=security_protocol
        )
        
        # Check if topic already exists
        existing_topics = admin_client.list_topics()
        if topic_name in existing_topics:
            logger.error(f"Topic '{topic_name}' already exists")
            admin_client.close()
            return False
        
        # Create topic
        topic_list = [
            NewTopic(
                name=topic_name,
                num_partitions=num_partitions,
                replication_factor=replication_factor,
                topic_configs=config
            )
        ]
        
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
        logger.info(f"Successfully created topic '{topic_name}'")
        
        # Close the admin client
        admin_client.close()
        return True
        
    except Exception as e:
        logger.error(f"Failed to create topic: {str(e)}")
        return False

def delete_topic(topic_name, bootstrap_servers=None, security_protocol="PLAINTEXT"):
    """
    Delete a Kafka topic
    
    Args:
        topic_name (str): The name of the topic to delete
        bootstrap_servers (str): Kafka broker(s) addresses (comma-separated list)
        security_protocol (str): Security protocol to use (default: "PLAINTEXT")
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Use environment variable if bootstrap_servers not provided
        if bootstrap_servers is None:
            bootstrap_servers = os.getenv('FD_KAFKA_BROKER', os.getenv('KAFKA_BROKER'))
            if not bootstrap_servers:
                logger.error("No Kafka bootstrap servers provided")
                return False
                
        logger.info(f"Deleting topic '{topic_name}'")
        
        # Create admin client
        admin_client = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            security_protocol=security_protocol
        )
        
        # Check if topic exists
        existing_topics = admin_client.list_topics()
        if topic_name not in existing_topics:
            logger.error(f"Topic '{topic_name}' does not exist")
            admin_client.close()
            return False
        
        # Delete topic
        admin_client.delete_topics([topic_name])
        logger.info(f"Successfully deleted topic '{topic_name}'")
        
        # Close the admin client
        admin_client.close()
        return True
        
    except Exception as e:
        logger.error(f"Failed to delete topic: {str(e)}")
        return False

def list_topics(bootstrap_servers=None, security_protocol="PLAINTEXT"):
    """
    List all available Kafka topics
    
    Args:
        bootstrap_servers (str): Kafka broker(s) addresses (comma-separated list)
        security_protocol (str): Security protocol to use (default: "PLAINTEXT")
        
    Returns:
        list: List of topic names if successful, empty list otherwise
    """
    try:
        # Use environment variable if bootstrap_servers not provided
        if bootstrap_servers is None:
            bootstrap_servers = os.getenv('FD_KAFKA_BROKER', os.getenv('KAFKA_BROKER'))
            if not bootstrap_servers:
                logger.error("No Kafka bootstrap servers provided")
                return []
                
        logger.info("Listing Kafka topics")
        
        # Create admin client
        admin_client = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            security_protocol=security_protocol
        )
        
        # Get topics
        topics = admin_client.list_topics()
        
        # Close the admin client
        admin_client.close()
        return topics
        
    except Exception as e:
        logger.error(f"Failed to list topics: {str(e)}")
        return [] 