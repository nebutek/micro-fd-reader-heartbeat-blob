import time
import os
import asyncio
from azure.eventhub import EventHubProducerClient, EventData
from azure.eventhub.aio import EventHubConsumerClient
from azure.eventhub.extensions.checkpointstoreblob import BlobCheckpointStore
from azure.storage.blob.aio import BlobServiceClient

class EventHubManager:
    def __init__(self, hub_name, consumer_group="$Default", read_connection_str=None, send_connection_str=None):
        """
        Initializes the EventHubSender with a connection to the specified Event Hub.
        
        :param hub_name: The name of the Event Hub.
        :param consumer_group: The consumer group to read from. Default is "$Default".
        :param read_connection_str: The connection string for reading from the Event Hub.
        :param send_connection_str: The connection string for sending to the Event Hub.
        """
        if send_connection_str is None:
            send_connection_str = os.getenv('FD_EVENT_HUB_SEND_CONNECTION_STRING')
        if read_connection_str is None:
            read_connection_str = os.getenv('FD_EVENT_HUB_READ_CONNECTION_STRING')
        if hub_name is None:
            hub_name = os.getenv('FD_EVENT_HUB_NAME')

        if not all([send_connection_str, read_connection_str, hub_name]):
            raise ValueError("Missing required environment variables. Please set FD_EVENT_HUB_SEND_CONNECTION_STRING, FD_EVENT_HUB_READ_CONNECTION_STRING, and FD_EVENT_HUB_NAME")

        self.send_connection_str = send_connection_str
        self.read_connection_str = read_connection_str
        self.hub_name = hub_name
        if consumer_group is None:
            self.consumer_group = os.getenv('FD_EVENT_HUB_CONSUMER_GROUP', "$default")
        else:
            self.consumer_group = consumer_group
        print(f"Consumer group: {self.consumer_group}")
        
        try:
            # Create the producer
            self.producer = EventHubProducerClient.from_connection_string(
                conn_str=self.send_connection_str,
                eventhub_name=self.hub_name
            )
            
            # Create the consumer
            self.consumer = EventHubConsumerClient.from_connection_string(
                conn_str=self.read_connection_str,
                consumer_group=self.consumer_group,
                eventhub_name=self.hub_name
            )
            
        except Exception as ex:
            print(f"Failed to connect to Event Hub '{self.hub_name}': {ex}")
            raise

    def send_event(self, event_data_list):
        """
        Sends a batch of events to the Event Hub and prints the time taken.
        
        :param event_data_list: A list of event data strings to send.
        """
        try:
            # Create a batch
            event_data_batch = self.producer.create_batch()

            # Add events to the batch
            for data in event_data_list:
                event_data_batch.add(EventData(data))

            # Measure the time taken to send the batch
            start_time = time.time()
            self.producer.send_batch(event_data_batch)
            end_time = time.time()

            # Calculate the elapsed time
            elapsed_time = end_time - start_time
            # print(f"Events sent successfully in {elapsed_time:.2f} seconds.")

        except Exception as ex:
            print(f"An error occurred: {ex}")

    async def read_event_async(self, callback=None, max_events=10, starting_position=None):
        """
        Asynchronously reads events from the Event Hub using the consumer and prints them.
        
        :param callback: Optional callback function that will be called for each event.
                       The callback should accept two parameters: partition_context and event.
        :param max_events: The maximum number of events to read. If -1, continues indefinitely.
        """
        print("Reading events from Event Hub...")
        
        async def on_event(partition_context, event):
            # print(f"on_event called for partition: {partition_context.partition_id}")
            if event is None:
                # print("Received null event")
                return
                
            if callback:
                # print(f"Calling callback for event sequence: {event.sequence_number}")
                callback(partition_context, event)
            else:
                print(f"No callback defined, event sequence: {event.sequence_number}")
                
            # print(f"Updating checkpoint for partition: {partition_context.partition_id}")
            await partition_context.update_checkpoint(event)

        try:
            print("Starting receive loop...")
            async with self.consumer:
                print(f"Consumer context entered, calling receive... consumer_group: {self.consumer_group}")
                if starting_position is not None:
                    await self.consumer.receive(
                        on_event=on_event,
                        starting_position=starting_position,
                        track_last_enqueued_event_properties=True,
                        max_wait_time=60
                    )
                else:
                    await self.consumer.receive(
                        on_event=on_event,
                        track_last_enqueued_event_properties=True,
                        max_wait_time=60
                    )
                print("receive() returned - this shouldn't happen unless there's an error")
        except Exception as ex:
            print(f"Error in read_event: {ex}")
            raise

    def read_event(self, callback=None, max_events=10, starting_position=None):
        """
        Synchronously reads events from the Event Hub using the consumer and prints them.
        
        :param callback: Optional callback function that will be called for each event.
                       The callback should accept two parameters: partition_context and event.
        :param max_events: The maximum number of events to read. If -1, continues indefinitely.
        """
        asyncio.run(self.read_event_async(callback, max_events, starting_position))

    def close(self):
        """
        Closes the connection to the Event Hub for both producer and consumer.
        """
        self.producer.close()
        self.consumer.close()
        print("Connections closed.")

# Example usage
if __name__ == "__main__":
    hub_name = "dev_edge_heartbeat"

    # Connection strings should be provided via environment variables or configuration
    # send_connection_str = "your_send_connection_string_here"
    # read_connection_str = "your_read_connection_string_here"
    # consumer = EventHubManager(hub_name, send_connection_str=send_connection_str, read_connection_str=read_connection_str, consumer_group="debug")

    # Send events
    # events = ["Event 1", "Event 2", "Event 3"]
    # sender.send_event(events)

    # Read events
    # def my_callback(partition_context, event):
    #     print(f"Custom handling of event: {event.body_as_str()}")
    #     print(f"Sequence number: {event.sequence_number}")

    # consumer.read_event(callback=my_callback)

    # Close the connection when done
    # consumer.close()

