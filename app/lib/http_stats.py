from prometheus_client import Counter, Histogram, Gauge

class HttpStats:
    def __init__(self, service_name: str, metrics_port: int = 9090):
        self.service_name = service_name
        self.metrics_port = metrics_port
        self._setup_metrics()

    def _setup_metrics(self):
        """Initialize Prometheus metrics for HTTP requests"""
        self.requests_total = Counter(
            'http_requests_total',
            'Total number of HTTP requests',
            ['service', 'endpoint', 'method', 'status_code']
        )
        self.request_duration = Histogram(
            'http_request_duration_seconds',
            'Time spent processing HTTP requests',
            ['service', 'endpoint', 'method'],
            buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0)
        )
        self.request_errors = Counter(
            'http_request_errors_total',
            'Total number of HTTP request errors',
            ['service', 'endpoint', 'method', 'error_type']
        )
        self.active_requests = Gauge(
            'http_active_requests',
            'Number of active HTTP requests',
            ['service', 'endpoint', 'method']
        )
        self.last_request_timestamp = Gauge(
            'http_last_request_timestamp_seconds',
            'Timestamp of the last HTTP request',
            ['service', 'endpoint', 'method']
        )

    def start_metrics_server(self):
        """Start the Prometheus metrics HTTP server (optional)"""
        from prometheus_client import start_http_server
        try:
            start_http_server(self.metrics_port)
            print(f"Prometheus metrics server started on port {self.metrics_port}")
        except Exception as e:
            print(f"Failed to start metrics server: {str(e)}")