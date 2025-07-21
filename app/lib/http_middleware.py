from fastapi import Request
from .http_stats import HttpStats
import time

async def http_middleware(request: Request, call_next, http_stats: HttpStats):
    """Middleware to track HTTP request metrics"""
    endpoint = request.url.path
    method = request.method
    start_time = time.time()

    # Increment active requests
    http_stats.active_requests.labels(
        service=http_stats.service_name, endpoint=endpoint, method=method
    ).inc()

    try:
        response = await call_next(request)
        status_code = response.status_code
        error_type = None
    except Exception as e:
        status_code = 500
        error_type = str(type(e).__name__)
        http_stats.request_errors.labels(
            service=http_stats.service_name, endpoint=endpoint, method=method, error_type=error_type
        ).inc()
        raise
    finally:
        # Decrement active requests
        http_stats.active_requests.labels(
            service=http_stats.service_name, endpoint=endpoint, method=method
        ).dec()

        # Record request duration
        duration = time.time() - start_time
        http_stats.request_duration.labels(
            service=http_stats.service_name, endpoint=endpoint, method=method
        ).observe(duration)

        # Record total requests
        http_stats.requests_total.labels(
            service=http_stats.service_name, endpoint=endpoint, method=method, status_code=str(status_code)
        ).inc()

        # Update last request timestamp
        http_stats.last_request_timestamp.labels(
            service=http_stats.service_name, endpoint=endpoint, method=method
        ).set(time.time())

    return response