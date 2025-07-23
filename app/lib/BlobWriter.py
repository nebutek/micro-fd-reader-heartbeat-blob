from datetime import datetime
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import threading
from threading import Lock
import copy
import json
import math, time
import os
import queue
import logging

# Initialize logger for this module
# Setup logging
log_level = os.getenv('FD_LOG_LEVEL', 'INFO').upper()
logging.basicConfig(
    level=getattr(logging, log_level),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)



# The QueueConsumer class is designed to handle message consumption from a queue, specifically tailored for environments that utilize message queuing protocols like AMQP, with RabbitMQ as a common example. It initializes with a queue name and a counter function, managing connections to a messaging server using credentials provided during instantiation. The class encapsulates the functionality to create a connection to the server, process incoming messages by appending them to an internal buffer, and manage the lifecycle of the connection and message consumption process.

# Key functionalities include:

class BlobWriter:
    def __init__(self, connection_string=None, container_name=None):
        # Use provided parameters or fall back to environment variables
        if connection_string is None:
            connection_string = os.getenv('FD_BLOB_CONNECTION_STRING')
            if connection_string is None:
                raise ValueError("Connection string must be provided either as parameter or via FD_BLOB_CONNECTION_STRING environment variable")
        
        if container_name is None:
            container_name = os.getenv('FD_BLOB_CONTAINER_NAME')
            if container_name is None:
                raise ValueError("Container name must be provided either as parameter or via FD_BLOB_CONTAINER_NAME environment variable")
        
        self.connection_string = connection_string
        self.container_name = container_name
        self.buffer = queue.Queue()
        self.blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)
        self.container_client = self.blob_service_client.get_container_client(self.container_name)
        self.running = False
        self.thread = None
        self.timer = False
        self.max_packet_size_bytes = 3 * 1024 * 1024

    def start(self):
        """Starts the background thread to write buffer to blob."""
        self.running = True
        self.timer = threading.Timer(1, self.run_periodically)
        self.timer.start()  # Start the timer immediately

    def run_periodically(self):
        """Sets up a timer to call write_buffer_to_blob periodically."""
        if self.running:
            self.write_buffer_to_blob()
            self.timer = threading.Timer(10, self.run_periodically)
            self.timer.start()

    def stop(self):
        """Stops the periodic writing process."""
        print('blobWriter.stop')
        self.running = False
        if self.timer:
            self.timer.cancel()
        self.write_buffer_to_blob()
        print('blobWriter.stop done writing blob')

    def list_blob_names(self):
        """
        Lists all blob names in the specified container.
        """
        try:
            blob_names = [blob.name for blob in self.container_client.list_blobs()]
            return blob_names
        except Exception as e:
            print(f"An error occurred while listing blobs: {e}")
            return []

    def create_append_blob(self, blob_name):
        """
        Creates an append blob in the specified container. If the blob already exists, this method does nothing.
        """
        try:
            if blob_name not in self.list_blob_names():
                blob_client = self.container_client.get_blob_client(blob_name)
                blob_client.create_append_blob()

        except Exception as e:
            print(f"An error occurred while creating the append blob: {e}")
    
    def append_data_to_blob(self, blob_name, data):
        """
        Appends data to an existing append blob. If the blob does not exist, it creates a new append blob.
        Splits data into packets to avoid exceeding the maximum append size.
        """
        packet_data=''
        try:
            blob_client = self.container_client.get_blob_client(blob_name)
            if blob_name not in self.list_blob_names():
                blob_client.create_append_blob()

            # Maximum size in bytes for a data packet (4 MB)
            max_packet_size_bytes = self.max_packet_size_bytes
            
            # Calculate packet size based on the size of the first record
            if data:
                packet_size = math.floor(max_packet_size_bytes / len(json.dumps(data[0]).encode('utf-8')))
            else:
                packet_size = 0  # No data to send

            packet_data=''
            # Append data in packets
            for i in range(0, len(data), packet_size):
                start_time = time.time()
                packet_data = data[i:i+packet_size]
                data_to_append = '\n'.join(json.dumps(item) for item in packet_data) + '\n'
                try:
                    blob_client.append_block(data_to_append.encode('utf-8'))
                    elapsed_time_ms = (time.time() - start_time) * 1000
                except Exception as e:
                    logging.error(f"An error occurred while appending data ({len(data_to_append)} bytes) to blob {blob_name}: {e}. ")
                    # print(f"Successfully appended {len(packet_data)} of {len(data)} records, {len(data_to_append)} bytes, to blob: {blob_name} in {elapsed_time_ms:.2f} ms")
        except Exception as e:
            logging.error(f"An error occurred while appending data ({len(packet_data)} ) to the blob {blob_name}: {e}. ")

    def write_buffer_to_blob(self):
        tmp_buffer = self.clear_buffer()
        blob_data = self.process_input_array(tmp_buffer)
        for data in blob_data:
            file_data = self.filter_by_type_and_date(tmp_buffer, data['type'], data['date'], data['time'])
            file_name = f"{data['type']}.{data['date']}.{data['time']}.fd"
            logging.info(f"Writing blob: {file_name}")
            self.create_append_blob(file_name)
            self.append_data_to_blob(file_name, file_data)

    def clear_buffer(self):
        tmp_buffer = []
        while not self.buffer.empty():
            tmp_buffer.append(self.buffer.get())
        return tmp_buffer

    def message_handler(self, message):
        self.buffer.put(message)

    def process_input_array(self, input_array):
        processed_array = []
        seen = set()  # Set to keep track of unique type-date combinations

        for item in input_array:
            type = item['data']['type']
            logged_at = item['meta']['timestamp']
            date_parts = logged_at.split('T')
            date = date_parts[0].replace('-', '')
            time = date_parts[1].split(':')[0]

            # Create a unique identifier for each type-date combination
            unique_id = f"{type}-{date}-{time}"
            # print(f'process_input_array {unique_id}')
            # Only add to the processed_array if this combination hasn't been seen before
            if unique_id not in seen:
                processed_array.append({"type": type, "date": date, "time": time})
                seen.add(unique_id)

        return processed_array       

    def filter_by_type_and_date(self, input_array, type_to_match, date_to_match, time_to_match):
        filtered_results = []
        for item in input_array:
            type = item['data']['type']
            logged_at = item['meta']['timestamp']
            date_parts = logged_at.split('T')
            date = date_parts[0].replace('-', '')
            time = date_parts[1].split(':')[0]


            # Check if the current item matches the type and date criteria
            if type == type_to_match and date == date_to_match and time == time_to_match:
                filtered_results.append(item)

        return filtered_results     