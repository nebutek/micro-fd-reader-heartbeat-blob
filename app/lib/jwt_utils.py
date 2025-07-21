import jwt
from datetime import datetime, timedelta
import os

def create_test_token(
    tenant_id: str = "werner",
    user_id: str = "test_user",
    expires_in_hours: int = 24
) -> str:
    """
    Create a test JWT token with standard claims and custom tenant/user data.
    
    Args:
        tenant_id: The tenant identifier
        user_id: The user identifier
        expires_in_hours: Token expiration time in hours
        
    Returns:
        str: The encoded JWT token
    """
    # Get the secret key from environment or use a test key
    secret_key = os.getenv('JWT_SECRET_KEY', 'test_secret_key')
    
    # Current time
    now = datetime.utcnow()
    
    # Token payload
    payload = {
        # Standard JWT claims
        'iat': now,  # Issued at
        'exp': now + timedelta(hours=expires_in_hours),  # Expiration
        'sub': user_id,  # Subject (user)
        
        # Custom claims
        'tenant_id': tenant_id,
        'user_id': user_id,
        'role': 'admin',
        'permissions': ['read', 'write', 'admin']
    }
    
    # Create the token
    token = jwt.encode(
        payload,
        secret_key,
        algorithm='HS256'
    )
    
    return token

def verify_token(token: str) -> dict:
    """
    Verify and decode a JWT token.
    
    Args:
        token: The JWT token to verify
        
    Returns:
        dict: The decoded token payload
        
    Raises:
        jwt.InvalidTokenError: If the token is invalid
    """
    secret_key = os.getenv('JWT_SECRET_KEY', 'test_secret_key')
    
    try:
        payload = jwt.decode(
            token,
            secret_key,
            algorithms=['HS256']
        )
        return payload
    except jwt.ExpiredSignatureError:
        raise ValueError("Token has expired")
    except jwt.InvalidTokenError as e:
        raise ValueError(f"Invalid token: {str(e)}")

# Example usage
if __name__ == "__main__":
    # Simple test token
    secret_key = 'test_secret_key'
    payload = {
        'tenant_id': 'werner',
        'user_id': 'test123',
        'exp': datetime.utcnow() + timedelta(hours=24)
    }
    
    token = jwt.encode(payload, secret_key, algorithm='HS256')
    print("Test Token:", token) 