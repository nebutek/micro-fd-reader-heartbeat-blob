# /lib/tenant_connect_manager/__init__.py
from .manager import TenantConnectManager
import jwt  # or your preferred JWT lib

def set_active_connection_middleware(req):
    auth_header = req.headers.get("authorization", "")
    token = auth_header.replace("Bearer ", "")
    if token:
        decoded = jwt.decode(token, options={"verify_signature": False})  # Use key for real
        tenant_id = decoded.get("tenant_id")
        if tenant_id:
            TenantConnectManager.set_active_connection(tenant_id)
