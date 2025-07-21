from dataclasses import dataclass
from typing import Dict, Any, Optional
import arrow
import logging

logger = logging.getLogger(__name__)

@dataclass
class HOSEvent:
    # Driver information
    driver_id: str
    driver_external_id: str
    driver_name: str
    event_timestamp_utc: arrow.Arrow
    
    # Vehicle information
    asset_external_id: str
    vin: str
    
    # Status information
    malfunction_status: int
    malfunction_code: Optional[str]
    diagnostic_status: int
    diagnostic_code: Optional[str]
    comment: Optional[str]
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> Optional['HOSEvent']:
        try:
            # Validate basic structure
            if not isinstance(data, dict):
                logger.error("Input data is not a dictionary")
                return None
                
            if 'data' not in data:
                logger.error("Missing 'data' key in input")
                return None

            # Get the attributes directly from data.attributes
            attrs = data['data'].get('attributes', {})
            if not attrs:
                logger.error("Missing attributes in data")
                return None

            # Get driver information from relationships
            relationships = data['data'].get('relationships', {})
            users_data = relationships.get('users', {}).get('data', [])
            driver = None
            for user in users_data:
                if user and isinstance(user, dict) and user.get('type') == 'user':
                    driver = user
                    break
            
            if not driver:
                logger.error("No valid driver found in data")
                return None
                
            driver_attrs = driver.get('attributes', {})
            
            # Get the event timestamp from attributes
            event_timestamp = attrs.get('event_timestamp_utc')
            if not event_timestamp:
                logger.error("Missing event_timestamp_utc in attributes")
                return None
            
            # Create the event with safe gets and defaults
            return cls(
                # Driver info
                driver_id=str(driver.get('id', '')),
                driver_external_id=str(driver_attrs.get('external_id', '')),
                driver_name=f"{driver_attrs.get('first_name', '')} {driver_attrs.get('last_name', '')}".strip(),
                event_timestamp_utc=arrow.get(event_timestamp),
                
                # Vehicle info
                asset_external_id=str(attrs.get('power_unit_number', '')),
                vin=str(attrs.get('vin', '')),
                
                # Status info with safe type conversion
                malfunction_status=int(attrs.get('malfunction_status', 0)),
                malfunction_code=attrs.get('malfunction_code'),
                diagnostic_status=int(attrs.get('diagnostic_status', 0)),
                diagnostic_code=attrs.get('diagnostic_code'),
                comment=attrs.get('comment')
            )
            
        except Exception as e:
            logger.error(f"Error parsing HOS event: {str(e)}")
            return None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary format"""
        try:
            return {
                'driver_id': self.driver_id,
                'driver_external_id': self.driver_external_id,
                'driver_name': self.driver_name,
                'event_timestamp_utc': self.event_timestamp_utc.format('YYYY-MM-DD HH:mm:ss'),
                'asset_external_id': self.asset_external_id,
                'vin': self.vin,
                'malfunction_status': self.malfunction_status,
                'malfunction_code': self.malfunction_code,
                'diagnostic_status': self.diagnostic_status,
                'diagnostic_code': self.diagnostic_code,
                'comment': self.comment
            }
        except Exception as e:
            logger.error(f"Error converting HOS event to dict: {str(e)}")
            return {}

    def is_valid(self) -> bool:
        """Check if the event has all required fields populated"""
        return all([
            self.driver_id,
            self.driver_external_id,
            self.driver_name,
            self.event_timestamp_utc,
            self.asset_external_id,
            self.vin
        ])