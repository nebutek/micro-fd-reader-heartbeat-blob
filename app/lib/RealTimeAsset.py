import json
from lib.RedisDockerClient import RedisDockerClient

class RealTimeAsset:
    
    def __init__(self):
        self.redis_client = RedisDockerClient()

    def get_asset(self, asset_id=None, asset_external_id=None, vin=None):
        if asset_id:
            try:
                doc = self.redis_client.client.json().get('asset:' + asset_id)
                return doc if doc else None
            except Exception as e:
                print(f"Error getting asset by ID: {e}")
                return None
        elif asset_external_id and vin:
            results = self.redis_client.search("asset_idx", f'@asset_external_id:"{asset_external_id}" @vin:"{vin}"')
        elif asset_external_id:
            results = self.redis_client.search("asset_idx", f'@asset_external_id:"{asset_external_id}"')
        elif vin:
            results = self.redis_client.search("asset_idx", f'@vin:"{vin}"')
        else:
            return None
        if len(results) > 0:
            return json.loads(results[0].json)
        else:
            return None
        
# Add a main function to test the class
if __name__ == "__main__":
    asset = RealTimeAsset()
    print('asset_id:2732704190700438')
    print(asset.get_asset(asset_id="2732704190700438"))
    print('asset_external_id:21435')
    print(asset.get_asset(asset_external_id="21435"))
    print('vin:3AKJHHDR5NSMT3952')
    print(asset.get_asset(vin="3AKJHHDR5NSMT3952"))
    print('asset_external_id:21435 and vin:3AKJHHDR5NSMT3952')
    print(asset.get_asset(asset_external_id="21435", vin="3AKJHHDR5NSMT3952"))
