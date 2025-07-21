from dataclasses import dataclass, asdict
from typing import List, Optional

import arrow

def ignore_unknown_fields(cls):
    """Decorator to make dataclasses ignore unknown fields"""
    original_init = cls.__init__
    def __init__(self, **kwargs):
        # Filter out unknown fields
        known_fields = cls.__annotations__.keys()
        filtered_kwargs = {k: v for k, v in kwargs.items() if k in known_fields}
        original_init(self, **filtered_kwargs)
    cls.__init__ = __init__
    return cls

@ignore_unknown_fields
@dataclass
class UserDetails:
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    age: Optional[int] = None
    certifications: Optional[str] = None
    phone_number: Optional[str] = None
    fax: Optional[str] = None
    drivers_license_number: Optional[str] = None
    drivers_license_state: Optional[str] = None

@ignore_unknown_fields
@dataclass
class UserInfo:
    id: Optional[int] = None
    external_id: Optional[str] = None
    email: Optional[str] = None
    account_status: Optional[bool] = None
    username: Optional[str] = None
    details: Optional[UserDetails] = None

@ignore_unknown_fields
@dataclass
class AssetInfo:
    asset_id: Optional[str] = None
    vin: Optional[str] = None
    external_asset_id: Optional[str] = None
    asset_external_id: Optional[str] = None
    serial_cvd: Optional[str] = None

@ignore_unknown_fields
@dataclass
class Meta:
    user_info: Optional[UserInfo] = None
    asset_info: Optional[AssetInfo] = None

@ignore_unknown_fields
@dataclass
class BluetoothDevice:
    deviceName: Optional[str] = None
    deviceAddress: Optional[str] = None
    deviceAlias: Optional[str] = None
    deviceUuids: Optional[str] = None
    deviceBondState: Optional[int] = None
    deviceType: Optional[str] = None
    deviceMajorClass: Optional[str] = None

@ignore_unknown_fields
@dataclass
class WifiNetwork:
    frequency: Optional[int] = None
    signalLevel: Optional[int] = None
    bssid: Optional[str] = None
    BSSID: Optional[str] = None
    ssid: Optional[str] = None
    SSID: Optional[str] = None

    def __post_init__(self):
        # Handle case differences in field names
        if self.BSSID and not self.bssid:
            self.bssid = self.BSSID
        if self.SSID and not self.ssid:
            self.ssid = self.SSID

@ignore_unknown_fields
@dataclass
class WifiConnection:
    ipAddress: Optional[str] = None
    linkSpeedMbps: Optional[int] = None
    frequencyMHz: Optional[int] = None
    signalStrengthRSSI: Optional[int] = None
    isApprovedNetwork: Optional[bool] = None
    bssid: Optional[str] = None
    BSSID: Optional[str] = None
    ssid: Optional[str] = None
    SSID: Optional[str] = None

    def __post_init__(self):
        # Handle case differences in field names
        if self.BSSID and not self.bssid:
            self.bssid = self.BSSID
        if self.SSID and not self.ssid:
            self.ssid = self.SSID

@ignore_unknown_fields
@dataclass
class MobileData:
    operatorName: Optional[str] = None
    networkType: Optional[str] = None
    signalStrengthRSSI: Optional[int] = None
    isRoaming: Optional[bool] = None

@ignore_unknown_fields
@dataclass
class CVD:
    is_external_power_connected: Optional[bool] = None
    jbus_connected: Optional[bool] = None
    cvd_status: Optional[str] = None

@ignore_unknown_fields
@dataclass
class Bluetooth:
    nearbyDevices: Optional[List[BluetoothDevice]] = None
    devicesConnected: Optional[List[BluetoothDevice]] = None
    requestParingDevice: Optional[List[BluetoothDevice]] = None
    devicesDisconnected: Optional[List[BluetoothDevice]] = None
    requestDisconnectDevice: Optional[List[BluetoothDevice]] = None

@ignore_unknown_fields
@dataclass
class Wifi:
    nearbyWifi: Optional[List[WifiNetwork]] = None
    wifiConnected: Optional[WifiConnection] = None
    mobileData: Optional[MobileData] = None

@ignore_unknown_fields
@dataclass
class Cyber:
    bluetooth: Optional[Bluetooth] = None
    wifi: Optional[Wifi] = None
    cvd: Optional[CVD] = None

@ignore_unknown_fields
@dataclass
class RelativePosition:
    distance: Optional[str] = None
    unit_of_measure: Optional[str] = None
    direction: Optional[str] = None
    city: Optional[str] = None
    state_code: Optional[str] = None
    country_code: Optional[str] = None

@ignore_unknown_fields
@dataclass
class Location:
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    description: Optional[str] = None
    country_code: Optional[str] = None
    state_code: Optional[str] = None
    relative_position: Optional[RelativePosition] = None

@ignore_unknown_fields
@dataclass
class Message:
    type: Optional[str] = None
    id: Optional[str] = None
    event: Optional[str] = None
    meta: Optional[Meta] = None
    cyber: Optional[Cyber] = None
    location: Optional[Location] = None

class EdgeHeartbeat:
    def __init__(self, message: dict):
        # print('EdgeHeartbeat:init')
        self.raw_data = message
        self._heartbeat: Message = self._parse(message)
        # Create direct property access
        message_content = message.get('data', message)
        attributes = message_content.get('attributes', {})
        
        # Direct access to common properties
        self.id = message_content.get('id')
        self.type = message_content.get('type')
        self.event = attributes.get('event')
        self.logged_at = attributes.get('logged_at')
        self.location = attributes.get('location', {})
        self.latitude = attributes.get('location', {}).get('latitude')
        self.longitude = attributes.get('location', {}).get('longitude')
        self.odometer = attributes.get('odometer')
        self.engine_hours = attributes.get('engine_hours')
        self.fuel_level = attributes.get('fuel_level')
        self.logged_at_ts = arrow.get(self.logged_at).timestamp() if self.logged_at else None

        
        self.organization = ''

        # Direct access to meta properties
        meta = message.get('meta', {})
        asset_info = meta.get('asset_info', {})
        self.vin = asset_info.get('vin')
        self.user_info = meta.get('user_info')
        self.asset_info = asset_info
        
        self.asset_id = asset_info.get('asset_id', None)
        # Handle both external_asset_id and asset_external_id
        self.asset_external_id = asset_info.get('asset_external_id') or asset_info.get('external_asset_id')
        
        # Map driver information
        user_info = meta.get('user_info', {})
        self.driver_id = user_info.get('id')
        self.driver_external_id = user_info.get('external_id')
        
        # Direct access to cyber properties
        cyber = message.get('cyber', {})
        self.bluetooth = cyber.get('bluetooth')
        self.wifi = cyber.get('wifi', {})
        self.cvd = cyber.get('cvd')
        
        # Fix: Access wifi properties through the wifi object correctly
        if self.wifi:
            self.wifi_connected = self.wifi.get('wifiConnected')
            self.wifi_disconnected = self.wifi.get('wifiDisconnected')
        else:
            self.wifi_connected = None
            self.wifi_disconnected = None


    def _parse(self, message: dict) -> Message:
        # print('EdgeHeartbeat:_parse')
        # Extract the actual message content from the 'data' wrapper
        message_content = message.get('data', message)
        
        # Get meta and cyber from the root level of the message
        meta_data = message.get('meta', {})
        cyber_data = message.get('cyber', {})
        attributes = message_content.get('attributes', {})

        # Parse Meta
        user_details = UserDetails(**meta_data['user_info']['details'])
        user_info = UserInfo(**{**meta_data['user_info'], 'details': user_details})
        asset_info = AssetInfo(**meta_data['asset_info'])
        meta = Meta(user_info=user_info, asset_info=asset_info)

        # Parse Cyber
        bluetooth_data = cyber_data['bluetooth']
        bluetooth = Bluetooth(
            nearbyDevices=[BluetoothDevice(**device) for device in bluetooth_data['nearbyDevices']],
            devicesConnected=[BluetoothDevice(**device) for device in bluetooth_data.get('devicesConnected', [])],
            requestParingDevice=[BluetoothDevice(**device) for device in bluetooth_data.get('requestParingDevice', [])],
            devicesDisconnected=[BluetoothDevice(**device) for device in bluetooth_data.get('devicesDisconnected', [])],
            requestDisconnectDevice=[BluetoothDevice(**device) for device in bluetooth_data.get('requestDisconnectDevice', [])]
        )

        wifi_data = cyber_data['wifi']
        wifi = Wifi(
            nearbyWifi=[WifiNetwork(**network) for network in wifi_data['nearbyWifi']],
            wifiConnected=WifiConnection(**wifi_data['wifiConnected']),
            mobileData=MobileData(**wifi_data['mobileData'])
        )

        cvd = CVD(**cyber_data['cvd'])
        cyber = Cyber(bluetooth=bluetooth, wifi=wifi, cvd=cvd)

        # Create and return complete Message object
        return Message(
            type=message_content.get('type', ''),
            id=message_content.get('id', ''),
            event=attributes.get('event', ''),
            meta=meta,
            cyber=cyber
        )

    def get_alert(self) -> dict:
        # print('EdgeHeartbeat:get_alert')
        # Modified to use self.heartbeat instead of self.message
        if 'wifi' in self.event:
            sub_category = "WIFI"
        elif 'bluetooth' in self.event:
            sub_category = "BLUETOOTH"
        elif 'cvd' in self.event:
            sub_category = "CVD"
        else:
            sub_category = "UNKNOWN"

        # Define alert details based on events
        event_details = {
            "cyber_wifi_connect": {
                "fd_alert_type": "Info",
                "fd_alert_message": "Successfully connected to WiFi network",
                "details": self.wifi_connected
            },
            "cyber_wifi_connect_unauthorized": {
                "fd_alert_type": "Critical",
                "fd_alert_message": "Connected to unauthorized WiFi network",
                "details": self.wifi_connected
            },
            "cyber_wifi_disconnected": {
                "fd_alert_type": "Warning",
                "fd_alert_message": "Disconnected from WiFi network",
                "details": self.wifi_disconnected
            },
            "cyber_bluetooth_connected": {
                "fd_alert_type": "Info",
                "fd_alert_message": "Bluetooth device successfully connected",
                "details": self.bluetooth.get('devicesConnected') if self.bluetooth else None
            },
            "cyber_bluetooth_disconnected": {
                "fd_alert_type": "Warning",
                "fd_alert_message": "Bluetooth device disconnected",
                "details": self.bluetooth.get('devicesDisconnected') if self.bluetooth else None
            },
            "cyber_cvd_status": {
                "fd_alert_type": "Critical",
                "fd_alert_message": "The CVD has lost communication with the JBUS",
                "details": self.cvd
            },
            "cyber_cvd_success": {
                "fd_alert_type": "Info",
                "fd_alert_message": "CVD operation completed successfully",
                "details": self.cvd
            },
            "cyber_cvd_jbus_connect": {
                "fd_alert_type": "Info",
                "fd_alert_message": "JBUS connection established successfully",
                "details": self.cvd
            },
            "cyber_cvd_jbus_disconnect": {
                "fd_alert_type": "Info",
                "fd_alert_message": "JBUS connection has been lost",
                "details": self.cvd                
            },
            'cyber_cvd_socket_failure': {
                "fd_alert_type": "Critical",
                "fd_alert_message": "The CVD recieved a socket failure",
                "details": self.cvd
            },
            "cyber_cvd_is_external_power_connected": {
                "fd_alert_type": "Info",
                "fd_alert_message": "The power has been restored to the CVD",
                "details": self.cvd
            },
            "cyber_cvd_is_external_power_disconnected": {
                "fd_alert_type": "Critical",
                "fd_alert_message": "The power has been disconnected from the CVD",
                "details": self.cvd
            }
        }

    
        # Get event details or use defaults
        details = event_details.get(self.event, {
            "fd_alert_type": "Info",
            "fd_alert_message": f"Unknown cyber event: {self.event}",
            "details": []
        })

        # Check  details.details is and array, if not make it and array and loop through create multiple alerts
        if not isinstance(details["details"], list):
            details["details"] = [details["details"]]

        alerts = []
        # add an index and append the value to the id
        for index, detail in enumerate(details["details"]):
            detail['serial_cvd'] = self.asset_info.get('serial_cvd')
            id = f"{self.id}"
            if index > 0:
                id = f"{self.id}-{index}"               
            alert = {
                "fd_id": f"{id}",
                "fd_alert": self.event,
                'ps_asset_id': f"{self.asset_id}",
                "ps_user_id": f"{self.driver_id}",
                "ps_driver_id": f"{self.driver_id}",
                "ps_vin": self.vin,
                "fd_ts": self.logged_at,
                "ps_ts": self.logged_at,
                "ps_event": self.event,
                "ps_type": self.type,
                "fd_alert_type": details["fd_alert_type"],
                "category": "Cyber",
                "sub_category": sub_category,
                "fd_alert_message": details["fd_alert_message"],
                "details": detail,
                "latitude": self.latitude if self.latitude else None,
                "longitude": self.longitude if self.longitude else None,
                "location": self.location,
                "auto_resolved": False
            }
            alerts.append(alert)
        return alerts
