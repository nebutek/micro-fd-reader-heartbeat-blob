import sys
import os
import json
import arrow
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))
from lib.MongoDBDockerClient import MongoDBDockerClient
from lib.EdgeHeartbeat import EdgeHeartbeat
from lib.RedisDockerClient import RedisDockerClient


class RealTimeEdgeHeartbeat:
    def __init__(self):
        try:
            self.redis_client = RedisDockerClient()
            self.mongo_client = MongoDBDockerClient(collection_name="edge_heartbeats")
        except Exception as e:
            print(f"Failed to initialize RedisDBClient: {e}")
            raise

    def save_edge_heartbeat(self, edge_heartbeat: EdgeHeartbeat):
        key = f'cyber:{edge_heartbeat.asset_id}'
        # Convert timestamp to ISO format for Redis
        message = {
            'asset_id': f'{edge_heartbeat.asset_id}',
            'asset_external_id': edge_heartbeat.asset_external_id,
            'driver_id': f'{edge_heartbeat.driver_id}',
            'driver_external_id': edge_heartbeat.driver_external_id,
            'organization': edge_heartbeat.organization,
            'timestamp': edge_heartbeat.logged_at_ts,
            'vin': edge_heartbeat.vin,
            'data': edge_heartbeat.raw_data
        }
        try:
            # print(key, message.get('timestamp'), edge_heartbeat.logged_at)
            self.redis_client.insert(key, message)
        except Exception as e:
            print(f"Failed to save edge heartbeat: {e}")

    def save_edge_heartbeat_mongo(self, edge_heartbeat: EdgeHeartbeat):
        # Convert to proper date type for MongoDB
        timestamp = arrow.get(edge_heartbeat.logged_at).datetime if edge_heartbeat.logged_at else None
        message = {
            "id": edge_heartbeat.id,
            'asset_id': f'{edge_heartbeat.asset_id}',
            'asset_external_id': edge_heartbeat.asset_external_id,
            'driver_id': f'{edge_heartbeat.driver_id}',
            'driver_external_id': edge_heartbeat.driver_external_id,
            'organization': edge_heartbeat.organization,
            'timestamp': timestamp,
            'vin': edge_heartbeat.vin,
            'data': {
                'cyber': edge_heartbeat.raw_data.get('cyber', {}),
                'meta': edge_heartbeat.raw_data.get('meta', {}),
                'attributes': edge_heartbeat.raw_data.get('data', {}).get('attributes', {})
            }
        }
        try:
            # print(key, message.get('timestamp'), edge_heartbeat.logged_at)
            self.mongo_client.insert(message)
        except Exception as e:
            print(f"Failed to save edge heartbeat: {e}")

        
# Add a main function to test the class
# if __name__ == "__main__":
    # asset = RealTimeAsset()
    # print('asset_id:2732704190700438')
    # print(asset.get_asset(asset_id="2732704190700438"))
    # print('asset_external_id:21435')
    # print(asset.get_asset(asset_external_id="21435"))
    # print('vin:3AKJHHDR5NSMT3952')
    # print(asset.get_asset(vin="3AKJHHDR5NSMT3952"))
    # print('asset_external_id:21435 and vin:3AKJHHDR5NSMT3952')
    # print(asset.get_asset(asset_external_id="21435", vin="3AKJHHDR5NSMT3952"))
