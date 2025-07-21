import os
import redis
import logging
import json
from datetime import datetime
from redis.commands.search.query import Query
import time

class RedisDockerClient:
    def __init__(self, host=None, port=6379, db=0):
        """
        Initialize Redis client
        
        Args:
            host (str): Redis host
            port (int): Redis port
            db (int): Redis database number
        """
        self.logger = logging.getLogger(__name__)
        self.host = host or os.getenv('REDIS_DB_HOST', 'localhost')
        self.port = port or int(os.getenv('REDIS_DB_PORT', 6379))
        self.db = db or int(os.getenv('REDIS_DB', 0))
        self.password = os.getenv('REDIS_DB_PASSWORD', '')
        self.client = None
        print('--------------------------------')
        print('host', self.host, 'port', self.port, 'db', self.db, 'password', self.password)
        print('--------------------------------')
        self.connect()

    def connect(self):
        """Connect to Redis"""
        try:
            self.client = redis.Redis(
                host=self.host,
                port=self.port,
                db=self.db,
                password=self.password,
                decode_responses=True,
                socket_timeout=5,
                socket_connect_timeout=5
            )
            self.client.ping()
            self.logger.info(f"Connected to Redis at {self.host}:{self.port}")
        except Exception as e:
            self.logger.error(f"Failed to connect to Redis: {str(e)}")
            raise

    def get(self, key):
        """
        Get value from Redis
        
        Args:
            key (str): Redis key
            
        Returns:
            str: Redis value
        """
        try:
            return self.client.get(key)
        except Exception as e:
            self.logger.error(f"Failed to get key {key}: {str(e)}")
            return None

    def set(self, key, value, ex=None):
        """
        Set value in Redis
        
        Args:
            key (str): Redis key
            value (str): Redis value
            ex (int): Expiration time in seconds
        """
        try:
            self.client.set(key, value, ex=ex)
        except Exception as e:
            self.logger.error(f"Failed to set key {key}: {str(e)}")

    def delete(self, key):
        """
        Delete key from Redis
        
        Args:
            key (str): Redis key
        """
        try:
            self.client.delete(key)
        except Exception as e:
            self.logger.error(f"Failed to delete key {key}: {str(e)}")

    def exists(self, key):
        """
        Check if key exists in Redis
        
        Args:
            key (str): Redis key
            
        Returns:
            bool: True if key exists, False otherwise
        """
        try:
            return self.client.exists(key)
        except Exception as e:
            self.logger.error(f"Failed to check if key {key} exists: {str(e)}")
            return False

    def find_one(self, key):
        """Get a single record by its key.
        
        Args:
            key (str): The Redis key for the record
            
        Returns:
            dict: The record if found, None if not found or on error
        """
        try:
            if not self.client.exists(key):
                return None
            return self.select(key)
        except Exception as e:
            print(f"Redis Error getting record {key}: {e}")
            return None
        
    def search(self, index_name, query_string, limit=10, **kwargs):
        """
        Search JSON documents using RediSearch with improved error handling.

        Args:
            index_name (str): Name of the index to search
            query_string (str): Search query string (can include @field:"value" syntax)
            limit (int): Maximum number of results to return
            **kwargs: Additional search parameters (not used in redis-py 5.x+)

        Returns:
            list: Search results
        """
        max_retries = 3
        retry_delay = 1  # seconds

        for attempt in range(max_retries):
            try:
                # Create a Query object with the raw query string
                q = Query(query_string)
                # Add paging
                q.paging(0, limit)
                # Execute the search
                return self.client.ft(index_name).search(q).docs
            except redis.TimeoutError as e:
                if attempt == max_retries - 1:
                    self.logger.error(f"Redis Timeout Error after {max_retries} attempts: {e}")
                    return []
                self.logger.warning(f"Redis Timeout Error (attempt {attempt + 1}/{max_retries}): {e}")
                time.sleep(retry_delay * (attempt + 1))
            except redis.ConnectionError as e:
                self.logger.error(f"Redis Connection Error: {e}")
                return []
            except Exception as e:
                self.logger.error(f"Redis Error performing search: {e}")
                return []

    def create_index(self, index_name, schema):
        """
        Create a RediSearch index
        
        Args:
            index_name (str): Name of the search index
            schema (dict): Index schema
        """
        try:
            from redis.commands.search import Search
            search = Search(self.client, index_name)
            search.create_index(schema)
        except Exception as e:
            self.logger.error(f"Failed to create index {index_name}: {str(e)}")

    def drop_index(self, index_name):
        """
        Drop a RediSearch index
        
        Args:
            index_name (str): Name of the search index
        """
        try:
            from redis.commands.search import Search
            search = Search(self.client, index_name)
            search.drop_index()
        except Exception as e:
            self.logger.error(f"Failed to drop index {index_name}: {str(e)}")

    def close(self):
        """Close Redis connection"""
        if self.client:
            self.client.close()

    def insert(self, key, data):
        """
        Insert data into Redis
        
        Args:
            key (str): Redis key
            data (dict): Data to insert
        """
        try:
            self.client.json().set(key, '$', data)
        except Exception as e:
            self.logger.error(f"Failed to insert data into Redis: {str(e)}")
            raise 