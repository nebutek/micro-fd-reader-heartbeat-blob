from pymongo import MongoClient
import time
from functools import wraps
from urllib.parse import quote_plus
import os

import logging

log_level = os.getenv('FD_LOG_LEVEL', 'INFO').upper()
logging.basicConfig(
    level=getattr(logging, log_level),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(os.getenv('FD_SERVICE_NAME', 'fd-simulate-reader'))


def time_operation(operation_name):
    def decorator(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            start_time = time.time()
            result = func(self, *args, **kwargs)
            elapsed_time = time.time() - start_time
            self._update_stats(operation_name, elapsed_time)
            return result
        return wrapper
    return decorator

class MongoDBDockerClient:
    _client_pool = None

    def __init__(self, db_name=None, 
                 collection_name="heartbeats", 
                 connection_string=None,
                 max_pool_size=10,
                 username=None,
                 password=None,
                 host=None,
                 port=None,
                 current_db=None):
        # Get settings from environment with overrides from parameters
        username = username or os.environ.get('MONGO_DB_USERNAME', '')
        password = password or os.environ.get('MONGO_DB_PASSWORD', '')
        db_name = db_name or os.environ.get('MONGO_DB_DATABASE', 'telematics')
        host = host or os.environ.get('MONGO_DB_HOST', 'localhost')
        port = port or os.environ.get('MONGO_DB_PORT', '27017')

        logger.info('--------------------------------')
        logger.info(f"Connecting to MongoDB: {host}:{port}")
        logger.info(f"Database: {db_name}")
        logger.info(f"Collection: {collection_name}")
        logger.info('--------------------------------')

        self.db_name = db_name
        self.collection_name = collection_name

        # If connection is provided, use it directly as the client pool
        if current_db is not None:
            self.db = current_db
            self.collection = self.db[collection_name]
            # Update db_name to reflect the actual database name from the provided connection
            self.db_name = self.db.name
            logger.info(f"Using provided database connection for: {self.db.name}.{self.collection.name}")
        else:
            # Use provided connection string or build from components
            if connection_string is None:
                # Build connection string based on whether authentication is provided
                if username and password:
                    # URL encode username and password for special characters
                    encoded_username = quote_plus(username)
                    encoded_password = quote_plus(password)
                    connection_string = f'mongodb://{encoded_username}:{encoded_password}@{host}:{port}'
                else:
                    # No authentication
                    connection_string = f'mongodb://{host}:{port}'

            if MongoDBDockerClient._client_pool is None:
                try:
                    MongoDBDockerClient._client_pool = MongoClient(
                        connection_string,
                        maxPoolSize=max_pool_size
                    )
                    # Test the connection
                    MongoDBDockerClient._client_pool.admin.command('ping')
                    logger.info("Successfully connected to MongoDB")
                except Exception as e:
                    logger.error(f"Error connecting to MongoDB: {e}")
                    raise

            self.db = MongoDBDockerClient._client_pool[db_name]
            self.collection = self.db[collection_name]
            logger.info(f"Connected to MongoDB: {self.db.name}.{self.collection.name}")
        
        # Add statistics tracking
        self.stats = {
            'insert': {'count': 0, 'total_time': 0, 'min_time': float('inf'), 'max_time': 0},
            'find': {'count': 0, 'total_time': 0, 'min_time': float('inf'), 'max_time': 0},
            'update': {'count': 0, 'total_time': 0, 'min_time': float('inf'), 'max_time': 0},
            'delete': {'count': 0, 'total_time': 0, 'min_time': float('inf'), 'max_time': 0}
        }

    def set_collection(self, collection_name):  
        self.collection = self.db[collection_name]
        logger.info(f"Set collection to: {self.db.name}.{self.collection.name}")

    def _update_stats(self, operation, elapsed_time):
        """Update statistics for an operation."""
        stats = self.stats[operation]
        stats['count'] += 1
        stats['total_time'] += elapsed_time
        stats['min_time'] = min(stats['min_time'], elapsed_time)
        stats['max_time'] = max(stats['max_time'], elapsed_time)

    def get_stats(self, operation=None):
        """Get statistics for one or all operations."""
        if operation:
            stats = self.stats[operation]
            avg_time = stats['total_time'] / stats['count'] if stats['count'] > 0 else 0
            return {
                'operation': operation,
                'count': stats['count'],
                'total_time': stats['total_time'],
                'avg_time': avg_time,
                'min_time': stats['min_time'] if stats['min_time'] != float('inf') else 0,
                'max_time': stats['max_time']
            }
        return {op: self.get_stats(op) for op in self.stats.keys()}

    @time_operation('insert')
    def insert(self, document):
        """Insert a document into the MongoDB collection."""
        try:
            logger.info(f"Inserting document {self.db.name}.{self.collection.name}")
            mongo_doc = document.copy()
            result = self.collection.insert_one(mongo_doc)
            logger.info(f"Insert result: {result.acknowledged}")
            return result.acknowledged
        except Exception as e:
            logger.error(f"Error inserting document: {e}")
            return False

    @time_operation('find')
    def find(self, filter_dict=None, sort_field=None, limit=None, skip=None):
        """
        Find documents with optional filtering, sorting, limiting, and skipping.
        
        Args:
            filter_dict (dict): MongoDB filter criteria (default: None)
            sort_field (tuple or list): Tuple/list of (field, direction) or just field name (default: None)
            limit (int): Maximum number of documents to return (default: None)
            skip (int): Number of documents to skip (default: None)
        """
        try:
            # Start with base query
            cursor = self.collection.find(filter_dict if filter_dict else {})
            
            # Apply sorting if specified
            if sort_field:
                # Handle both string and tuple/list inputs
                if isinstance(sort_field, (tuple, list)):
                    cursor = cursor.sort(*sort_field)
                else:
                    cursor = cursor.sort(sort_field)
            
            # Apply skip if specified
            if skip:
                cursor = cursor.skip(skip)
            
            # Apply limit if specified
            if limit:
                cursor = cursor.limit(limit)
            
            return list(cursor)
        except Exception as e:
            logger.error(f"Error executing find query: {e}")
            return []

    @time_operation('update')
    def update(self, document_id, updated_document):
        """Update a document in the MongoDB collection."""
        try:
            result = self.collection.update_one(
                {"_id": document_id}, 
                {"$set": updated_document}
            )
            return result.modified_count > 0
        except Exception as e:
            logger.error(f"Error updating document: {e}")
            return False

    @time_operation('delete')
    def delete(self, document_id):
        """Delete a document from the MongoDB collection."""
        try:
            result = self.collection.delete_one({"_id": document_id})
            return result.deleted_count > 0
        except Exception as e:
            logger.error(f"Error deleting document: {e}")
            return False

    @time_operation('find')
    def aggregate(self, group_by_fields, metrics, filter_dict=None):
        """
        Perform aggregation operations with grouping and metrics.
        
        Args:
            group_by_fields (str or list): Field(s) to group by
            metrics (list of dict): List of metrics to calculate
                Each metric should be a dict with:
                - 'operation': 'max', 'min', 'avg', or 'sum'
                - 'field': field name to perform operation on
            filter_dict (dict): Optional filter criteria before aggregation
        
        Example:
            client.aggregate(
                group_by_fields=['category', 'region'],
                metrics=[
                    {'operation': 'sum', 'field': 'amount'},
                    {'operation': 'avg', 'field': 'price'},
                    {'operation': 'max', 'field': 'quantity'}
                ],
                filter_dict={'date': {'$gte': '2024-01-01'}}
            )
        """
        try:
            # Convert single field to list for consistent handling
            if isinstance(group_by_fields, str):
                group_by_fields = [group_by_fields]

            # Prepare the group _id
            group_id = {field: f"${field}" for field in group_by_fields}
            
            # Prepare the group metrics
            group_metrics = {}
            for metric in metrics:
                operation = metric['operation'].lower()
                field = metric['field']
                output_name = f"{operation}_{field}"
                
                if operation == 'sum':
                    group_metrics[output_name] = {'$sum': f"${field}"}
                elif operation == 'avg':
                    group_metrics[output_name] = {'$avg': f"${field}"}
                elif operation == 'max':
                    group_metrics[output_name] = {'$max': f"${field}"}
                elif operation == 'min':
                    group_metrics[output_name] = {'$min': f"${field}"}
                else:
                    raise ValueError(f"Unsupported operation: {operation}")

            # Build the pipeline
            pipeline = []
            
            # Add match stage if filter is provided
            if filter_dict:
                pipeline.append({'$match': filter_dict})
            
            # Add group stage
            pipeline.append({
                '$group': {
                    '_id': group_id,
                    **group_metrics
                }
            })

            # Execute aggregation
            result = list(self.collection.aggregate(pipeline))
            
            # Transform the result to make it more readable
            transformed_result = []
            for doc in result:
                transformed_doc = {}
                # Add grouping fields
                for field in group_by_fields:
                    transformed_doc[field] = doc['_id'][field]
                # Add metrics
                transformed_doc.update({k: v for k, v in doc.items() if k != '_id'})
                transformed_result.append(transformed_doc)
                
            return transformed_result

        except Exception as e:
            logger.error(f"Error executing aggregation: {e}")
            return []

    @time_operation('find')
    def find_one(self, filter_dict=None):
        """
        Find a single document matching the filter criteria.
        
        Args:
            filter_dict (dict): MongoDB filter criteria (default: None)
            
        Returns:
            dict: The matching document or None if not found
        """
        try:
            return self.collection.find_one(filter_dict if filter_dict else {})
        except Exception as e:
            logger.error(f"Error executing find_one query: {e}")
            return None

    @time_operation('find')
    def query(self, operation, *args, **kwargs):
        """
        Execute any MongoDB collection operation directly.
        
        Args:
            operation (str): The MongoDB operation to execute (e.g., 'find', 'distinct', 'count_documents')
            *args: Positional arguments to pass to the operation
            **kwargs: Keyword arguments to pass to the operation
            
        Returns:
            The result of the operation or None if operation fails
            
        Example:
            # Get distinct values
            client.query('distinct', 'field_name')
            
            # Count documents
            client.query('count_documents', {'status': 'active'})
            
            # Find with complex query
            client.query('find', {'field': {'$gt': 100}}, {'_id': 0})
        """
        try:
            collection_method = getattr(self.collection, operation)
            result = collection_method(*args, **kwargs)
            
            # Convert cursor to list if the operation returns a cursor
            if operation == 'find':
                result = list(result)
                
            return result
        except Exception as e:
            logger.error(f"Error executing {operation} query: {e}")
            return None

# Main for testing
def main():
    # Updated connection string with authentication
    connection_string = "mongodb://admin:secretpassword@localhost:27017"
    
    mongo_client = MongoDBDockerClient(
        db_name="test_db", 
        collection_name="test_collection",
        connection_string=connection_string
    )
    
    test_document = {"_id": 1, "name": "John Doe", "email": "john@example.com"}
    updated_document = {"name": "Johnathan Doe", "email": "johnathan.doe@example.com"}

    # Insert
    logger.info("Inserting document...")
    if mongo_client.insert(test_document):
        logger.info("Document inserted successfully.")

    # Find by ID
    logger.info("Finding document by ID...")
    document = mongo_client.find_by_id(1)
    if document:
        logger.info(f"Document found: {document}")

    # Update
    logger.info("Updating document...")
    if mongo_client.update(1, updated_document):
        logger.info("Document updated successfully.")

    # Find again to verify update
    logger.info("Finding document after update...")
    document = mongo_client.find_by_id(1)
    if document:
        logger.info(f"Updated document: {document}")

    # Delete
    logger.info("Deleting document...")
    if mongo_client.delete(1):
        logger.info("Document deleted successfully.")

    # Find after delete to verify
    logger.info("Finding document after delete...")
    document = mongo_client.find_by_id(1)
    if not document:
        logger.info("Document not found (as expected).")

if __name__ == "__main__":
    main()
