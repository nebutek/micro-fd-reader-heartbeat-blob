import threading
from lib.tenant_connect.manager_blob import TenantBlobManager

# Mapping of tenant_id to BlobWriter
blob_writers = {}
blob_threads = {}

def initialize_blob_writers():
    global blob_writers, blob_threads
    blob_writers = TenantBlobManager._connections
    for tenant_id, blob_writer in blob_writers.items():
        t = threading.Thread(
            name=f"blobWriter-{tenant_id}",
            target=blob_writer.start,
            daemon=True
        )
        blob_threads[tenant_id] = t
        t.start()

def dispatch_blob_message(tenant_id, message):
    if tenant_id in blob_writers:
        blob_writers[tenant_id].message_handler(message)
    else:
        raise KeyError(f"No BlobWriter for tenant {tenant_id}") 