import logging
import json
import time
import os
from typing import Optional, Any
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewPartitions, NewTopic
from .kafka_admin import list_topics

logger = logging.getLogger(__name__)

class KafkaProducerManager:
    def __init__(self, topic: Optional[str] = None, bootstrap_servers: Optional[str] = None):
        """Initialize the Kafka Producer Manager.
        
        Args:
            topic: The Kafka topic to produce to. Falls back to KAFKA_TOPIC env var if not provided.
            bootstrap_servers: The Kafka broker(s) to connect to. Falls back to KAFKA_BROKER env var if not provided.
        """
        self.bootstrap_servers = bootstrap_servers or os.getenv('KAFKA_BROKER', 'kafka:9092')
        self.kafka_topic = topic or os.getenv('KAFKA_TOPIC', 'telematics.raw')
        self.kafka_username = os.getenv('KAFKA_USERNAME', 'user1')
        self.kafka_password = os.getenv('KAFKA_PASSWORD', '')
        self.producer: Optional[KafkaProducer] = None
        
        if not self.bootstrap_servers:
            raise ValueError("No bootstrap servers provided and KAFKA_BROKER environment variable is not set")
        if not self.kafka_topic:
            raise ValueError("No topic provided and KAFKA_TOPIC environment variable is not set")
            
    def initialize(self, partitions: int = 1) -> bool:
        """Initialize the Kafka producer and configure the topic."""
        try:
            if self.producer is None:
                logger.info(f"Initializing Kafka producer with broker: {self.bootstrap_servers}")
                
                # Check if our topic exists and get its information
                try:
                    topics = list_topics(bootstrap_servers=self.bootstrap_servers)
                    logger.info(f"Available Kafka topics: {topics}")
                    
                    if self.kafka_topic in topics:
                        logger.info(f"Topic '{self.kafka_topic}' found in Kafka")
                    else:
                        logger.info(f"Topic '{self.kafka_topic}' not found in Kafka. Creating it...")
                        self._create_topic(self.kafka_topic, partitions)
                except Exception as topic_error:
                    logger.warning(f"Could not list Kafka topics: {str(topic_error)}")
                    # Try to create the topic anyway
                    self._create_topic(self.kafka_topic, partitions)
                
                # Set up partitions if needed
                self._configure_partitions(self.kafka_topic, partitions)
                
                # Initialize the producer
                self.producer = KafkaProducer(
                    bootstrap_servers=self.bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    security_protocol="PLAINTEXT",
                    retries=5,
                    acks='all',
                    compression_type='gzip',
                    batch_size=16384,
                    linger_ms=100,
                    buffer_memory=33554432
                )
                
                # Test the connection
                try:
                    metadata = self.producer.partitions_for(self.kafka_topic)
                    logger.info(f"Successfully connected to Kafka and verified topic access. Topic metadata: {metadata}")
                    return True
                except Exception as topic_error:
                    logger.error(f"Connected to Kafka broker but topic '{self.kafka_topic}' not found or not accessible: {str(topic_error)}")
                    raise
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize producer: {str(e)}")
            self.producer = None
            return False
    
    def _create_topic(self, topic_name: str, num_partitions: int) -> bool:
        """Create a new Kafka topic if it doesn't exist."""
        try:
            logger.info(f"Creating topic '{topic_name}' with {num_partitions} partitions")
            
            admin_client = KafkaAdminClient(
                bootstrap_servers=self.bootstrap_servers,
                security_protocol="PLAINTEXT"
            )
            
            # Create the new topic
            new_topic = NewTopic(
                name=topic_name,
                num_partitions=num_partitions,
                replication_factor=1  # Default to 1 for development
            )
            
            admin_client.create_topics([new_topic])
            logger.info(f"Successfully created topic '{topic_name}'")
            
            admin_client.close()
            return True
            
        except Exception as e:
            logger.error(f"Failed to create topic: {str(e)}")
            return False
    
    def _configure_partitions(self, topic_name: str, new_partition_count: int) -> bool:
        """Configure the number of partitions for a topic."""
        try:
            logger.info(f"Configuring partitions for topic '{topic_name}' to {new_partition_count}")
            
            admin_client = KafkaAdminClient(
                bootstrap_servers=self.bootstrap_servers,
                security_protocol="PLAINTEXT"
            )
            
            topic_info = admin_client.describe_topics([topic_name])
            current_partitions = len(topic_info[0]['partitions'])
            
            if new_partition_count <= current_partitions:
                logger.info(f"Current partition count ({current_partitions}) is already >= requested count ({new_partition_count})")
                return True
                
            topic_partitions = {
                topic_name: NewPartitions(total_count=new_partition_count)
            }
            
            admin_client.create_partitions(topic_partitions)
            logger.info(f"Successfully changed partition count for topic '{topic_name}' from {current_partitions} to {new_partition_count}")
            
            admin_client.close()
            return True
            
        except Exception as e:
            logger.error(f"Failed to change partition count: {str(e)}")
            return False
    
    def send_message(self, message: Any) -> bool:
        """Send a message to Kafka, handling initialization if needed."""
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                # Ensure producer is initialized
                if self.producer is None and not self.initialize():
                    raise Exception("Failed to initialize producer")
                
                future = self.producer.send(self.kafka_topic, message)
                record_metadata = future.get(timeout=10)
                logger.info(f"Message sent successfully - Topic: {record_metadata.topic}, Partition: {record_metadata.partition}, Offset: {record_metadata.offset}")
                self.producer.flush()
                return True
                
            except Exception as e:
                logger.error(f"Error sending message (attempt {retry_count + 1}/{max_retries}): {str(e)}")
                self.producer = None
                retry_count += 1
                if retry_count < max_retries:
                    time.sleep(1)
        
        return False
    
    def close(self):
        """Close the Kafka producer connection."""
        if self.producer:
            self.producer.close()
            self.producer = None 