# Fleet Defender Heartbeat Reader Service

A microservice that consumes telematics heartbeat data from Kafka and stores it in MongoDB with multi-tenant support.

## Overview

The Fleet Defender Heartbeat Reader Service is a Python-based microservice designed to process real-time telematics heartbeat data from vehicles. It acts as a data ingestion pipeline that:

1. **Consumes** heartbeat messages from Kafka topics
2. **Processes** and validates the telemetry data
3. **Stores** the data in MongoDB collections with tenant isolation
4. **Provides** Prometheus metrics for monitoring and observability

## Architecture

### Core Components

- **Kafka Consumer**: Handles message consumption from Kafka topics with automatic rebalancing and error handling
- **MongoDB Client**: Manages database connections and operations with connection pooling
- **Tenant Manager**: Provides multi-tenant support with isolated database connections per tenant
- **Metrics Server**: Exposes Prometheus metrics on port 9090 for monitoring
- **Message Processor**: Handles individual message processing with timestamp conversion and validation

### Data Flow

```
Kafka Topic → KafkaReader → Message Processor → Tenant Manager → MongoDB Collection
     ↓              ↓              ↓                ↓              ↓
  Raw Data    Deserialization  Validation    Tenant Routing   Persistent Storage
```

## Features

### Multi-Tenant Support
- Isolated database connections per tenant
- Thread-safe connection management
- Automatic tenant routing based on message metadata

### Monitoring & Observability
- Prometheus metrics for message processing
- Consumer lag monitoring
- Processing time histograms
- Error tracking and alerting

### Resilience & Reliability
- Automatic retry mechanisms with exponential backoff
- Failsafe mode to prevent service crashes
- Connection pooling for database operations
- Graceful shutdown handling

### Configuration Management
- Environment-based configuration
- Kubernetes ConfigMap and Secret integration
- Flexible Kafka topic and consumer group configuration

## Prerequisites

- Python 3.12+
- Docker
- Azure Container Registry (ACR)
- Azure Kubernetes Service (AKS)
- Kafka cluster
- MongoDB instance

## Installation & Deployment

### Local Development

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd micro-fd-reader-heartbeat-blob
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Set environment variables**
   ```bash
   export FD_SERVICE_NAME=fd-reader-heartbeat-blob
   export FD_KAFKA_TOPIC=telematics.raw.telematics_heartbeat
   export FD_MONGO_COLLECTION_NAME=heartbeats
   export KAFKA_BROKER=kafka:9092
   export KAFKA_USERNAME=your_kafka_username
   export KAFKA_PASSWORD=your_kafka_password
   export MONGO_DB_HOST=localhost
   export MONGO_DB_PORT=27017
   export MONGO_DB_DATABASE=telematics
   export MONGO_DB_USERNAME=your_mongo_username
   export MONGO_DB_PASSWORD=your_mongo_password
   ```

4. **Run the service**
   ```bash
   python app/main.py
   ```

### Docker Deployment

1. **Build the Docker image**
   ```bash
   docker build -t fd-reader-heartbeat-blob:latest .
   ```

2. **Run the container**
   ```bash
   docker run -p 9090:9090 \
     -e KAFKA_BROKER=kafka:9092 \
     -e KAFKA_USERNAME=your_username \
     -e KAFKA_PASSWORD=your_password \
     -e MONGO_DB_HOST=mongo:27017 \
     fd-reader-heartbeat-blob:latest
   ```

### Kubernetes Deployment

1. **Deploy to AKS using the provided script**
   ```bash
   ./deploy.sh
   ```

   This script will:
   - Build and tag the Docker image
   - Push to Azure Container Registry
   - Deploy to AKS cluster
   - Apply Kubernetes manifests

2. **Manual deployment**
   ```bash
   kubectl apply -f aks/manifests/deployment.yaml
   ```

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `FD_SERVICE_NAME` | Service name for logging and metrics | `fd-reader-heartbeat-blob` |
| `FD_KAFKA_TOPIC` | Kafka topic to consume from | `telematics.raw.telematics_heartbeat` |
| `FD_MONGO_COLLECTION_NAME` | MongoDB collection name | `heartbeats` |
| `FD_LOG_LEVEL` | Logging level | `INFO` |
| `FD_METRICS_PORT` | Prometheus metrics port | `9090` |
| `FD_MAX_RETRIES` | Maximum retry attempts | `5` |
| `FD_RETRY_DELAY` | Retry delay in seconds | `30` |
| `KAFKA_BROKER` | Kafka broker address | `kafka:9092` |
| `KAFKA_USERNAME` | Kafka username | `user1` |
| `KAFKA_PASSWORD` | Kafka password | (required) |
| `MONGO_DB_HOST` | MongoDB host | `localhost` |
| `MONGO_DB_PORT` | MongoDB port | `27017` |
| `MONGO_DB_DATABASE` | MongoDB database name | `telematics` |
| `MONGO_DB_USERNAME` | MongoDB username | (optional) |
| `MONGO_DB_PASSWORD` | MongoDB password | (optional) |

### Message Format

The service expects heartbeat messages in the following JSON format:

```json
{
  "tenant_id": "tenant123",
  "asset_id": "vehicle456",
  "driver_id": "driver789",
  "driver_name": "John Doe",
  "timestamp": "2025-06-23T00:37:01.000Z",
  "location": {
    "latitude": 40.7128,
    "longitude": -74.0060
  },
  "status": "active",
  "additional_data": {}
}
```

## Monitoring

### Prometheus Metrics

The service exposes the following metrics on port 9090:

- `messages_processed_total`: Total number of processed messages
- `processing_errors_total`: Total number of processing errors
- `message_processing_duration_seconds`: Message processing time histogram
- `last_message_timestamp_seconds`: Timestamp of last processed message
- `consumer_lag`: Kafka consumer lag per partition
- `assigned_partitions_count`: Number of assigned partitions

### Health Checks

- **Metrics endpoint**: `http://localhost:9090/metrics`
- **Service health**: Monitor pod status in Kubernetes

### Logging

The service uses structured logging with the following levels:
- `INFO`: Normal operation messages
- `ERROR`: Error conditions and exceptions
- `DEBUG`: Detailed debugging information

## Development

### Project Structure

```
├── app/
│   ├── main.py                 # Main application entry point
│   └── lib/                    # Core library modules
│       ├── kafka_reader.py     # Kafka consumer implementation
│       ├── MongoDBDockerClient.py  # MongoDB client wrapper
│       ├── tenant_connect/     # Multi-tenant connection management
│       └── ...                 # Additional utility modules
├── aks/
│   └── manifests/              # Kubernetes deployment manifests
├── Dockerfile                  # Docker image definition
├── requirements.txt            # Python dependencies
├── deploy.sh                   # Deployment script
└── README.md                   # This file
```

### Testing

Run the test suite:
```bash
pytest test_python.py
```

### Code Quality

The project uses:
- **Black**: Code formatting
- **Flake8**: Linting
- **Pytest**: Testing framework

## Troubleshooting

### Common Issues

1. **Kafka Connection Issues**
   - Verify Kafka broker is accessible
   - Check authentication credentials
   - Ensure topic exists and is accessible

2. **MongoDB Connection Issues**
   - Verify MongoDB host and port
   - Check authentication credentials
   - Ensure database and collection exist

3. **Tenant Connection Issues**
   - Verify tenant connections are properly configured
   - Check tenant routing logic
   - Ensure tenant isolation is working

### Debug Mode

Enable debug logging:
```bash
export FD_LOG_LEVEL=DEBUG
```

### Log Retrieval

Use the provided log retrieval script:
```bash
python get_logs.py
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Ensure all tests pass
6. Submit a pull request

## License

[Add your license information here]

## Support

For support and questions, please contact the development team or create an issue in the repository. 