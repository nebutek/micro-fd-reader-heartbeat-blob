import os
import signal
import threading
import sys
from lib.MicroQueueConsumer import MicroQueueConsumer

class MicroConsumer:
    def __init__(self, message_handler, ps_host, is_staging=False, topics=None):
        self.status = 'initializing'
        self.message_handler = message_handler
        self.ps_host = ps_host
        self.is_staging = is_staging
        self.blob_thread = False
        
        # Default topics if none provided
        self.default_topics = [
            'external.fldf.telematics_heartbeat',
            'external.fldf.vehicle_fault',
            'external.fldf.critical_event'
        ]
        
        # Priority: 1. Constructor parameter, 2. Environment variable, 3. Default topics
        if topics is not None:
            self.topics = topics
        else:
            env_topics = os.getenv("topics")
            if env_topics is not None:
                self.topics = [topic.strip() for topic in env_topics.split(",")]
            else:
                self.topics = self.default_topics
 
        self.consumers = []
        self.consumer_threads = []
        self.timer = False

    def start_consumer(self):
        print('start_consumer')                
        # Initialize queues
        for topic in self.topics:
            consumer = MicroQueueConsumer(topic, 
                                  message_handler=self.message_handler, 
                                  ps_host=self.ps_host,
                                  is_staging=self.is_staging)
            self.consumers.append(consumer)
        
        # Create consumer threads
        for consumer in self.consumers:
            _thread = threading.Thread(name=consumer.queue_name, target=consumer.listen, daemon=True)
            _thread.start()
            self.consumer_threads.append(_thread)
        
        print('Active threads at startup:')
        for _thread in threading.enumerate():
            print(f"Active Threads {_thread.name} Is daemon: {_thread.isDaemon()}")
                
        signal.pause()  # Wait for a signal

    def stop_all(self):
        print('Active threads at shutdown:')
        for _thread in threading.enumerate():
            print(f"Active Theads {_thread.name} Is daemon: {_thread.isDaemon()}")

        if self.timer:
            self.timer.cancel()
        for consumer in self.consumers:
            consumer.stop()
        for _thread in self.consumer_threads:
            _thread.join(timeout=5)  # Optionally wait for the threads to finish

        self.blob.stop()
        self.blob_thread.join(timeout=5)  # Wait for the blob operation to finish    
        sys.exit(0) 