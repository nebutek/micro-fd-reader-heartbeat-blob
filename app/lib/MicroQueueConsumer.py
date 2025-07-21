from datetime import datetime
import json
import os
import pika
import threading
from threading import Lock
import copy
import time

# The QueueConsumer class is designed to handle message consumption from a queue, specifically tailored for environments that utilize message queuing protocols like AMQP, with RabbitMQ as a common example. It initializes with a queue name and a counter function, managing connections to a messaging server using credentials provided during instantiation. The class encapsulates the functionality to create a connection to the server, process incoming messages by appending them to an internal buffer, and manage the lifecycle of the connection and message consumption process.

# Key functionalities include:

# create_connection: Establishes a connection to the messaging server using the pika library and returns a channel for communication.
# process_messages_buffer: Acts as a callback for handling incoming messages, processing them according to the class's logic, and appending them to an internal buffer for later use or processing.
# appendBuffer and clear_buffer: Manage the internal buffer, with thread-safe operations to add messages to the buffer and clear it, respectively, using deep copy to prevent modification issues.
# close and stop: Handle the graceful shutdown of the connection and the message consuming process, ensuring resources are properly released.
# listen: Starts the message consumption process, setting up the necessary callbacks and entering a loop that keeps the consumer active until a stop condition is triggered.

# Consumer variables
username = 'fldf-consumer'
password = 'P*_zB8H3tgUQn9ogzbsv6@hY'
# queue_name = 'external.fldf.telematics_heartbeat'

class MicroQueueConsumer:
    def __init__(self, queue, message_handler=None, ps_host=None, is_staging=False):
        self.queue_name = queue
        self.message_handler = message_handler
        
        # RabbitMQ credentials
        self.username = 'fldf-consumer'
        self.password = 'P*_zB8H3tgUQn9ogzbsv6@hY'
        
        # Build connection string based on staging flag
        host = 'amqp-staging.pltsci.com' if is_staging else 'amqp.pltsci.com'
        self.connection_string = f'amqps://{self.username}:{self.password}@{host}:5671/{ps_host}'
        
        self.should_stop = threading.Event()
        self.buffer = []
        self.lock = Lock()
        self.message_delay = float(os.environ.get('PS_MESSAGE_DELAY', '.005'))
        self.message_capacity = int(os.environ.get('PS_MESSAGE_CAPACITY', '10'))
 
    def create_connection(self, url):
        connection_string = pika.URLParameters(url)
        self.connection = pika.BlockingConnection(connection_string)
        channel = self.connection.channel()
        return channel
       
    def process_messages_buffer(self, channel, method, properties, body):
        data = json.loads(body)
        if self.message_handler:
            self.message_handler(data)
 
        time.sleep(self.message_delay)          
        channel.basic_ack(delivery_tag=method.delivery_tag)   

    def appendBuffer(self, message):
        with self.lock:
            self.buffer.append(message)
    
    def clear_buffer(self):
        with self.lock:
            tmp_buffer = copy.deepcopy(self.buffer)
            self.buffer = []
            return tmp_buffer
 
    def close(self):
        print(f'stopping {self.queue_name}' )
        if self.channel is not None:
            self.channel.close()
        if self.connection is not None:
            self.connection.close()
 
    def stop(self):
        print(f'stopping {self.queue_name}' )
        self.should_stop.set()
        while not self.should_stop.is_set():
            print(f'waiting {self.queue_name}')
       
    def listen(self):
        print(f'Creating connection to RabbitMQ server {self.queue_name}')
        self.channel = self.create_connection(self.connection_string)
        self.channel.basic_qos(prefetch_count=self.message_capacity)
        self.channel.basic_consume(
            queue=self.queue_name,
            on_message_callback=self.process_messages_buffer,
            auto_ack=False
        )
 
        while not self.should_stop.is_set():
            self.channel.connection.process_data_events(time_limit=1)  # Process events for 1 second
        
        print(f'Stopping server {self.queue_name}')
        self.channel.stop_consuming()
        self.close()
 