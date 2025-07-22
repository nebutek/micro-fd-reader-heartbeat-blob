import json
import uuid
import arrow


class TelematicsHeartbeat:
    def __init__(self, data: json):
        self.id = data.get('data').get('id')
        self.type = data.get('data').get('type')
        self.attributes = data.get('data').get('attributes')
        self.relationships = data.get('data').get('relationships')
        self.meta = data.get('meta', {})
        self.event = self.attributes.get('event')
        self.location = self.attributes.get('location')
        self.logged_at = self.attributes.get('logged_at', self.meta.get('timestamp'))
        self.logged_at_unix = arrow.get(self.logged_at).int_timestamp
        self.meta_timestamp_unix = arrow.get(self.meta.get('timestamp', self.logged_at)).int_timestamp
        self.load_relationships()
        self.get_idle_druration()
        if self.location is not None:
            self.state_code   = self.location.get('state_code', None)
            self.country_code = self.location.get('country_code', None)
        else: 
            print('no location')
            print(json.dumps(data))
        
    def get_idle_druration(self):
        self.idle_duration = 0
        for i in self.attributes.get('idle_periods'):
            self.idle_duration =+ i.get('duration')

    def load_relationships(self):
        try:
            user = self.relationships.get("users").get("data")[0]
            self.driver_id = user.get('id')
            self.driver_external_id = user.get('attributes', {}).get('external_id', None)  
        except:
            print('no relationship found')
            self.driver_id = None
            self.driver_external_id = None
        # get device
        devices_data = self.relationships.get("devices").get("data")
        self.cvd_id = [device for device in devices_data if device["type"] == "cvd"][0].get('id')

        # get asset
        assets_data = self.relationships.get("assets").get("data")
        asset = [a for a in assets_data if a["type"] == "power_unit"][0]
        self.asset_external_id = asset.get('attributes',{}).get('external_id', None)
        self.vin = asset.get('attributes',{}).get('hardware_id', None)
        self.hardware_id = self.vin
        self.asset_id = asset.get('id')

    def get_alert(self, fd_alert, category, alert_severity, alert_message, alert_details = None):
      alert = {
            "fd_id": str(uuid.uuid4()),
            "fd_alert": fd_alert,
            "fd_alert_type": alert_severity,
            "category": category,
            "fd_alert_message": alert_message,
            "ps_id": self.id,
            "ps_type": self.type,
            "ps_cvd_id": self.cvd_id,
            "ps_vin": self.vin,
            "ps_asset_id": self.asset_id,
            "ps_asset_external_id": self.asset_external_id,
            "ps_user_id": self.driver_id,
            "ps_driver_external_id": self.driver_external_id,            
            "ps_ts": self.logged_at,
            "fd_ts": self.logged_at,
            "location": self.location,
            "details": {
                "speed": self.attributes.get('speed'),
                "event": self.event,
                "wheels_in_motion": self.attributes.get('wheels_in_motion'),
            },                       
            "show_notification": False
        }           
      if alert_details is not None:
        alert['details'] = alert_details
      return alert

if __name__ == "__main__":
    heartbeat = json.loads('{"data": {"type": "telematics_heartbeat", "id": "200000007065526414", "attributes": {"event": "periodic_update", "logged_at": "2024-10-30T23:59:51.000Z", "heartbeat_id": "200000007065526414", "speed": 60.7, "odometer": 621629.5, "odometer_jump": 0, "heading": 277, "ignition": true, "rpm": 1244.38, "engine_hours": 18668.2, "engine_hours_jump": 0, "wheels_in_motion": true, "accuracy": 1, "satellites": 11, "gps_valid": true, "gps": {"distance_diff": 4.86, "total_distance": 52.75}, "hdop": 80, "fuel_level": 35.2, "total_fuel_used": 88386.67, "switched_battery_voltage": 0, "ambient_temperature": 71.8, "location": {"latitude": 31.891466, "longitude": -102.516988, "description": "4 mi NNW of West Odessa, TX", "country_code": "US", "state_code": "TX", "relative_position": {"distance": "4", "unit_of_measure": "mi", "direction": "NNW", "city": "West Odessa", "state_code": "TX", "country_code": "US"}}, "idle_periods": [], "rsrp_rssi": -76, "rssi": -83, "store_and_forward": false}, "relationships": {"assets": {"data": [{"type": "power_unit", "id": "1345160245000967", "attributes": {"external_id": "16785", "hardware_id": "3AKJGLDR1JSHZ7160"}}]}, "devices": {"data": [{"type": "cvd", "id": "1493646973100135", "attributes": {"serial": "5571096977"}}, {"type": "tablet", "id": null, "attributes": {"serial": null}}]}, "users": {"data": [{"type": "user", "id": 1, "attributes": {"external_id": 2}}]}}}, "meta": {"message_id": "12bef3b0-971b-11ef-af51-158a37e5fe73", "consumer_version": "1.3.0", "origin_version": null, "timestamp": "2024-10-31T00:00:00.000Z"}}')
    th = TelematicsHeartbeat(heartbeat)
    print('logged_at', th.logged_at)
    print('driver_id', th.driver_id)
    print('driver_external_id', th.driver_external_id)
    print('vin', th.vin)
    print('asset_external_id', th.asset_external_id)
    print('event', th.event)
