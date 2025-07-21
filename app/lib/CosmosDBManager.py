from azure.cosmos import CosmosClient, PartitionKey, exceptions
import os
from datetime import datetime, timezone
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Suppress Azure SDK logging
logging.getLogger('azure.core.pipeline.policies.http_logging_policy').setLevel(logging.WARNING)
logging.getLogger('azure.cosmos').setLevel(logging.WARNING)

# Configure our application logger
logger = logging.getLogger('CosmosDBManager')

class CosmosDBManager:
    def __init__(self, container_name, partitionKey='fd_type', database_id=None):
        # Get settings from environment with overrides from parameters
        HOST = os.getenv('COSMOS_DB_HOST', 'localhost')
        MASTER_KEY = os.getenv('COSMOS_DB_MASTER_KEY', '')
        DATABASE_ID = database_id or os.getenv('COSMOS_DB_DATABASE', 'telematics')
        
        self.partitionKey = partitionKey
        self.container_name = container_name
        
        logger.info('-------------------------------- ')
        logger.info(f'HOST: {HOST}')
        logger.info(f'DATABASE_ID: {DATABASE_ID}')
        logger.info(f'container_name: {container_name}')
        logger.info(f'partitionKey: {partitionKey}')
        logger.info('--------------------------------')
        
        self.client = CosmosClient(HOST, credential=MASTER_KEY)
        self.database = self.client.get_database_client(DATABASE_ID)
        try:
            # First, explicitly check if database exists
            self.database = self.client.create_database_if_not_exists(DATABASE_ID)
            logger.info(f'Database {DATABASE_ID} connected/created')
            
            # Then try to create/connect to container
            self.container = self.database.create_container_if_not_exists(
                id=container_name,
                partition_key=PartitionKey(path=f"/{partitionKey}")  # Note the forward slash
            )
            logger.info(f'Container {container_name} connected/created')
            
        except exceptions.CosmosHttpResponseError as e:
            logger.error(f'Cosmos DB Error: {str(e)}')
            raise  # Re-raise the exception to see full error details

    def insert_data(self, data ):
        try:
            # Add a document to the container
            response = self.container.create_item(data)
            logger.info(f"Document added with ID: {response['id']}")
        except exceptions.CosmosHttpResponseError as e:
            logger.error(f"Error inserting data: {e}")
            pass

    def save_data(self, data):
        try:
            # Try to add a new document
            response = self.container.create_item(data)
            logger.info(f"Document added with ID: {response['id']}")
        except exceptions.CosmosHttpResponseError as e:
            if 'Conflict' in str(e):
                self.update_data(data)
            else:
                logger.error(f'Failed to add document: {e}')

    def update_data(self, data):
        try:
            # Read the existing document and update it
            read_item = self.container.read_item(item=f"{data['id']}", partition_key=data[self.partitionKey])
            for key, value in data.items():
                read_item[key] = value
            read_item["updated_at"] = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
            response = self.container.replace_item(item=read_item['id'], body=read_item)
            logger.info(f"Document updated with ID: {response['id']}")
        except exceptions.CosmosResourceNotFoundError:
            logger.warning(f"Document not found in the database. {data['id']} {data[self.partitionKey]}")
        except exceptions.CosmosHttpResponseError as e:
            logger.error(f'Failed to update document: {e}')

    def query(self, query, parameters=None):
        if parameters is not None:
            return list(self.container.query_items(
                query=query,
                parameters=parameters,
                enable_cross_partition_query=True
            ))
        else:
            return list(self.container.query_items(query=query))

    def delete_one(self, partition_key, id):
        try:
            # Attempt to delete the item
            self.container.delete_item(item=id, partition_key=partition_key)
            return True
        except Exception as e:
            logger.error(f"Unable to delete record with partition key: {partition_key} and id: {id}. Error: {e}")
            return False

    def find_one(self, partition_key, id ):
        try:
            item = self.container.read_item(item=id, partition_key=partition_key)
            return item
        except Exception as e:
            logger.warning(f"Unable to find record {partition_key} {id}")
            return None

    def find_many(self, partition_key: str, filters: dict = None):
        """
        Find multiple records in the database based on the partition key and filters.

        Args:
            partition_key (str): The partition key to use in the query.
            filters (dict, optional): A dictionary of filters to apply to the query. Defaults to None.

        Returns:
            list: A list of records that match the partition key and filters.
        """

        # Initial query with partition key
        items_query = """
        SELECT 
            *
        FROM c
        WHERE
            c.fd_type = @fd_type
        """
        items_parameters = [
            {"name": "@fd_type", "value": partition_key},
        ]

        # Check every filter and add it to the query
        if filters is not None:
            where_clauses = []

            for key, value in filters.items():
                param_name = f"@{key}"
                where_clauses.append(f"c.{key} = {param_name}")
                items_parameters.append({"name": param_name, "value": value})

            # Adding where clauses, ensuring no trailing "AND"
            if where_clauses:
                items_query += " AND " + " AND ".join(where_clauses)

        try:
            result = list(
                self.container.query_items(query=items_query, parameters=items_parameters)
            )
        except Exception as e:
            raise ValueError("Error querying items from the database") from e

        return result

# Example usage
if __name__ == "__main__":
    logger.info(datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z')
