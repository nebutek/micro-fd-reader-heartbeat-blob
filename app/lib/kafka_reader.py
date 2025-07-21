#!/usr/bin/env python3
"""
KafkaReader class for consuming messages from Kafka with Prometheus metrics
"""

import os
import logging
import time
import json
import threading
import socket
from typing import Callable, Dict, Any, Optional
from kafka import KafkaConsumer, TopicPartition
from kafka.consumer.subscription_state import ConsumerRebalanceListener
from prometheus_client import Counter, Histogram, Gauge, start_http_server

class LoggingConsumerRebalanceListener(ConsumerRebalanceListener):
    """Listener for consumer rebalance events"""
    def __init__(self, logger):
        self.logger = logger

    def on_partitions_revoked(self, revoked):
        """Called when partitions are revoked from this consumer"""
        self.logger.info(f"Partitions revoked: {revoked}")

    def on_partitions_assigned(self, assigned):
        """Called when partitions are assigned to this consumer"""
        self.logger.info(f"Partitions assigned: {assigned}")

class KafkaReader:
    """
    KafkaReader handles consuming messages from Kafka with metrics
    and executes a callback function for each message
    """
    
    def __init__(self, 
                 callback: Callable[[Dict[str, Any]], bool],
                 service_name: Optional[str] = None,
                 metrics_port: Optional[int] = None,
                 kafka_topic: Optional[str] = None,
                 consumer_group: Optional[str] = None,
                 auto_offset_reset: Optional[str] = None):
        """
        Initialize the KafkaReader
        
        Args:
            callback: Function to call for each message. Should return True on success, False on failure
            service_name: Name of the service (defaults to env var or 'kafka-reader')
            metrics_port: Port for Prometheus metrics (defaults to env var or 8000)
            consumer_group: Consumer group ID (defaults to env var or service_name)
            auto_offset_reset: Where to start reading from ('earliest', 'latest', or None for env var)
        """
        # Set up environment variables with defaults
        self.service_name = service_name or os.getenv('FD_SERVICE_NAME', 'kafka-reader')
        self.kafka_broker = os.getenv('KAFKA_BROKER', 'kafka:9092')
        if kafka_topic is None:
            self.kafka_topic = os.getenv('KAFKA_TOPIC', 'telematics.raw')
        else:
            self.kafka_topic = kafka_topic
        self.kafka_username = os.getenv('KAFKA_USERNAME', 'user1')
        self.kafka_password = os.getenv('KAFKA_PASSWORD', '')
        self.consumer_group = consumer_group or os.getenv('FD_CONSUMER_GROUP', self.service_name)
        if metrics_port is None:
            self.metrics_port = int(os.getenv('FD_METRICS_PORT', '9090'))
        else:
            self.metrics_port = metrics_port
        if auto_offset_reset is None:
            self.auto_offset_reset = os.getenv('KAFKA_AUTO_OFFSET_RESET', 'latest')
        else:
            self.auto_offset_reset = auto_offset_reset
        
        logging.info(f"KafkaReader initialized with service_name: {self.service_name}, kafka_broker: {self.kafka_broker}, kafka_topic: {self.kafka_topic}, consumer_group: {self.consumer_group}, metrics_port: {self.metrics_port}")

        # Setup logging
        log_level = os.getenv('FD_LOG_LEVEL', 'INFO').upper()
        logging.basicConfig(
            level=getattr(logging, log_level),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(self.service_name)
        
        # Store the callback function
        self.callback = callback
        
        # Initialize internal state
        self.stop_event = threading.Event()
        self.consumer_thread = None
        self.consumer = None
        
        # Set up metrics
        self.setup_metrics()
        self.start_metrics_server()
        
        # Validate configuration
        self.validate_config()
        
    def validate_config(self):
        """Validate that required configuration is present"""
        missing_configs = []
        if not self.kafka_topic:
            missing_configs.append("KAFKA_TOPIC")
        if not self.kafka_broker:
            missing_configs.append("KAFKA_BROKER")
        if not self.kafka_username:
            missing_configs.append("KAFKA_USERNAME")
        if not self.kafka_password:
            missing_configs.append("KAFKA_PASSWORD")
        
        if missing_configs:
            error_msg = f"Missing required configuration: {', '.join(missing_configs)}"
            self.logger.error(error_msg)
            raise ValueError(error_msg)
    
    def setup_metrics(self):
        """Initialize Prometheus metrics"""
        # Counter for total messages processed
        self.messages_processed = Counter(
            'messages_processed_total', 
            'Total number of processed messages', 
            ['service', 'topic', 'partition', 'success']
        )
        
        # Counter for processing errors
        self.processing_errors = Counter(
            'processing_errors_total', 
            'Total number of processing errors', 
            ['service', 'error_type']
        )
        
        # Histogram for message processing time
        self.processing_time = Histogram(
            'message_processing_duration_seconds', 
            'Time spent processing messages', 
            ['service', 'topic']
        )
        
        # Gauge for last message timestamp
        self.last_message_timestamp = Gauge(
            'last_message_timestamp_seconds', 
            'Timestamp of the last processed message', 
            ['service', 'topic', 'partition']
        )
        
        # Gauge for consumer lag (difference between latest offset and current position)
        self.consumer_lag = Gauge(
            'consumer_lag', 
            'Difference between latest offset and current position',
            ['service', 'topic', 'partition', 'group']
        )
        
        # Gauge for assigned partitions count
        self.assigned_partitions = Gauge(
            'assigned_partitions_count',
            'Number of partitions assigned to this consumer',
            ['service', 'topic', 'group']
        )
    
    def start_metrics_server(self):
        """Start the Prometheus metrics HTTP server"""
        try:
            start_http_server(self.metrics_port)
            self.logger.info(f"Prometheus metrics server started on port {self.metrics_port}")
        except Exception as e:
            self.logger.error(f"Failed to start metrics server: {str(e)}")
            # Continue without metrics
    
    def create_consumer(self):
        """Create a Kafka consumer with the proper configuration"""
        self.logger.info(f"Creating Kafka consumer for broker {self.kafka_broker} with group {self.consumer_group}")
        
        # Add connection diagnostics
        try:
            import socket
            ip_address = socket.gethostbyname('kafka')
            self.logger.info(f"Resolved kafka to IP: {ip_address}")
            
            # Try to establish a raw socket connection
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(10)
            result = s.connect_ex(('kafka', 9092))
            if result == 0:
                self.logger.info("Successfully connected to kafka:9092")
            else:
                self.logger.error(f"Failed to connect to kafka:9092. Error code: {result}")
            s.close()
        except Exception as e:
            self.logger.error(f"Connection test failed: {str(e)}")
        
        # Generate a unique client ID for debugging
        hostname = socket.gethostname()
        client_id = f"{self.service_name}-{hostname}-{int(time.time())}"
        
        self.logger.info(f"Attempting to create Kafka consumer with broker: {self.kafka_broker}")
        
        # Configure the Kafka consumer
        consumer = KafkaConsumer(
            bootstrap_servers=self.kafka_broker,
            group_id=self.consumer_group,
            client_id=client_id,
            auto_offset_reset=self.auto_offset_reset,
            enable_auto_commit=True,
            auto_commit_interval_ms=5000,
            security_protocol="PLAINTEXT",
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            session_timeout_ms=30000,
            heartbeat_interval_ms=10000,
            max_poll_interval_ms=300000,
            reconnect_backoff_ms=1000,
            reconnect_backoff_max_ms=30000,
            retry_backoff_ms=1000,
            request_timeout_ms=60000,
            connections_max_idle_ms=600000,
            metadata_max_age_ms=300000,
            api_version_auto_timeout_ms=60000  # Added timeout for API version detection
        )
        
        self.logger.info("Consumer instance created, attempting connection...")
        
        # Try to establish initial connection
        try:
            consumer.bootstrap_connected()
            self.logger.info(f"Successfully connected to Kafka broker: {self.kafka_broker}")
        except Exception as e:
            self.logger.error(f"Failed to connect to Kafka broker: {str(e)}")
            raise
        
        self.logger.info(f"Consumer created: client_id={client_id}, group_id={self.consumer_group}")
        return consumer
    
    def consume_messages(self, consumer, stop_event):
        """Consume messages from Kafka topic and process with callback"""
        # Subscribe to the topic with rebalance listener
        self.logger.info(f"Subscribing to topic: {self.kafka_topic}")
        consumer.subscribe(
            [self.kafka_topic],
            listener=LoggingConsumerRebalanceListener(self.logger)
        )
        
        # Process messages
        msg_count = 0
        try:
            while not stop_event.is_set():
                # Poll for messages with a timeout
                poll_result = consumer.poll(timeout_ms=1000)
                
                if not poll_result:
                    continue
                
                # Update metrics for assigned partitions
                assignment = consumer.assignment()
                self.assigned_partitions.labels(
                    service=self.service_name,
                    topic=self.kafka_topic,
                    group=self.consumer_group
                ).set(len(assignment))
                
                # Process received messages
                for tp, messages in poll_result.items():
                    for message in messages:
                        partition = message.partition
                        offset = message.offset
                        value = message.value
                        
                        # Log the message (debug level to avoid flooding logs)
                        self.logger.debug(f"Received message: partition={partition}, offset={offset}")
                        
                        # Update last message timestamp
                        self.last_message_timestamp.labels(
                            service=self.service_name,
                            topic=self.kafka_topic,
                            partition=partition
                        ).set(time.time())
                        
                        # Process the message with timing
                        start_time = time.time()
                        success = False
                        try:
                            # Call the callback function and track result
                            success = self.callback(value)
                            msg_count += 1
                        except Exception as e:
                            self.logger.error(f"Error processing message: {str(e)}", exc_info=True)
                            self.processing_errors.labels(
                                service=self.service_name,
                                error_type=type(e).__name__
                            ).inc()
                        finally:
                            # Record processing time
                            processing_duration = time.time() - start_time
                            self.processing_time.labels(
                                service=self.service_name,
                                topic=self.kafka_topic
                            ).observe(processing_duration)
                            
                            # Count processed messages
                            self.messages_processed.labels(
                                service=self.service_name,
                                topic=self.kafka_topic,
                                partition=partition,
                                success="true" if success else "false"
                            ).inc()
                
                # Update consumer lag metrics for each partition
                for tp in assignment:
                    try:
                        # Get the end offset and current position
                        end_offset = consumer.end_offsets([tp])[tp]
                        current_pos = consumer.position(tp)
                        lag = max(0, end_offset - current_pos)
                        
                        # Update the lag metric
                        self.consumer_lag.labels(
                            service=self.service_name,
                            topic=tp.topic,
                            partition=tp.partition,
                            group=self.consumer_group
                        ).set(lag)
                    except Exception as e:
                        self.logger.warning(f"Error calculating consumer lag: {str(e)}")
        except Exception as e:
            self.logger.error(f"Error consuming messages: {str(e)}", exc_info=True)
            self.processing_errors.labels(
                service=self.service_name,
                error_type=type(e).__name__
            ).inc()
        finally:
            self.logger.info(f"Consumer processed {msg_count} messages before exiting")
            consumer.close()
            self.logger.info("Consumer closed")
    
    def start(self):
        """Start consuming messages in a background thread"""
        self.logger.info(f"Starting KafkaReader for {self.service_name}")
        
        # Create consumer
        self.consumer = self.create_consumer()
        
        # Start consumer in a separate thread
        self.consumer_thread = threading.Thread(
            target=self.consume_messages,
            args=(self.consumer, self.stop_event)
        )
        self.consumer_thread.daemon = True
        self.consumer_thread.start()
        self.logger.info("Consumer thread started")
        
        return self
    
    def stop(self):
        """Stop the consumer thread"""
        self.logger.info("Stopping KafkaReader...")
        self.stop_event.set()
        
        if self.consumer_thread and self.consumer_thread.is_alive():
            self.consumer_thread.join(timeout=10)
            if self.consumer_thread.is_alive():
                self.logger.warning("Consumer thread did not terminate within timeout")
        
        self.logger.info("KafkaReader stopped")
    
    def is_running(self):
        """Check if the consumer thread is running"""
        return self.consumer_thread is not None and self.consumer_thread.is_alive()
    
    def monitor_and_restart(self, check_interval=60):
        """Monitor the consumer thread and restart if needed"""
        self.logger.info("Starting monitor thread...")
        
        while not self.stop_event.is_set():
            time.sleep(check_interval)
            
            if not self.is_running():
                self.logger.warning("Consumer thread is not running, restarting...")
                self.consumer = self.create_consumer()
                self.consumer_thread = threading.Thread(
                    target=self.consume_messages,
                    args=(self.consumer, self.stop_event)
                )
                self.consumer_thread.daemon = True
                self.consumer_thread.start()
                self.logger.info("Consumer thread restarted")
        
        self.logger.info("Monitor thread stopped")

    def run_forever(self):
        """Start the reader and keep the main thread running"""
        self.start()
        
        # Start the monitor thread
        monitor_thread = threading.Thread(
            target=self.monitor_and_restart
        )
        monitor_thread.daemon = True
        monitor_thread.start()
        
        try:
            # Keep the main thread running
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            self.logger.info("Interrupted, shutting down...")
            self.stop() 