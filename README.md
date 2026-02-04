# OME Kafka Telemetry to Grafana

Real-time monitoring and visualization solution for Dell OpenManage Enterprise (OME) telemetry data. This application consumes telemetry, alerts, and health data from Kafka topics, stores them in TimescaleDB, and visualizes them in Grafana dashboards.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    Dell OpenManage Enterprise (OME)                     │
│                                                                           │
│                  ┌──────────┐  ┌──────────┐  ┌──────────┐              │
│                  │Telemetry │  │  Alerts  │  │  Health  │              │
│                  └─────┬────┘  └────┬─────┘  └────┬─────┘              │
└────────────────────────┼────────────┼─────────────┼────────────────────┘
                         │            │             │
                         └────────────┴─────────────┘
                                      │
                                      ▼
                         ┌────────────────────────┐
                         │     Apache Kafka       │
                         │   Message Broker       │
                         ├────────────────────────┤
                         │  • ome/telemetry       │
                         │  • ome/alerts          │
                         │  • ome/health          │
                         └───────────┬────────────┘
                                     │
                                     ▼
                   ┌─────────────────────────────────┐
                   │   OME Kafka Consumer App        │
                   │  (Python + Confluent Kafka)     │
                   ├─────────────────────────────────┤
                   │  ┌──────────────────────────┐   │
                   │  │   OME Data Router        │   │
                   │  │  - Topic routing         │   │
                   │  │  - Data normalization    │   │
                   │  │  - Metric parsing        │   │
                   │  └──────────┬───────────────┘   │
                   │             │                   │
                   │  ┌──────────▼───────────────┐   │
                   │  │  OME Helper Service      │   │
                   │  │  - parse_telemetry_data  │   │
                   │  │  - normalize_alert_data  │   │
                   │  │  - normalize_health_data │   │
                   │  └──────────┬───────────────┘   │
                   └─────────────┼───────────────────┘
                                 │
                                 ▼
                   ┌─────────────────────────────────┐
                   │       TimescaleDB               │
                   │   (PostgreSQL + Time-Series)    │
                   ├─────────────────────────────────┤
                   │  Tables:                        │
                   │  • metrics (hypertable)     │
                   │  • alerts                   │
                   │  • health                   │
                   │  • devices                      │
                   └─────────────┬───────────────────┘
                                 │
                                 ▼
                   ┌─────────────────────────────────┐
                   │          Grafana                │
                   │    Visualization Dashboard      │
                   ├─────────────────────────────────┤
                   │  • Real-time metrics            │
                   │  • Alert monitoring             │
                   │  • Health status                │
                   │  • Historical analysis          │
                   │  • Custom queries               │
                   └─────────────────────────────────┘
```

## Features

### Data Ingestion
- **Real-time Kafka consumption** from multiple OME topics
- **Automatic topic routing** for telemetry, alerts, and health data
- **Graceful error handling** with comprehensive logging
- **Configurable consumer groups** and offset management
- **High throughput processing** with connection pooling

### Data Processing
- **Telemetry parsing** - Extracts individual metrics from OME telemetry payloads
- **Alert normalization** - Standardizes alert data across different formats
- **Health status tracking** - Monitors device health state changes
- **Data validation** - Ensures data integrity before storage
- **Flexible schema** - Supports dynamic OME data structures

### Time-Series Storage
- **TimescaleDB integration** - Optimized for time-series data
- **Hypertables** - Automatic data partitioning by time
- **Device tracking** - Maintains device inventory and metadata
- **Data retention policies** - Configurable data lifecycle management
- **Efficient indexing** - Fast queries on large datasets

### Visualization
- **Pre-configured Grafana dashboards** for OME telemetry
- **Real-time metrics** - Live updates as data arrives
- **Historical analysis** - Query and visualize historical trends
- **Alert visualization** - Monitor alert severity and frequency
- **Health monitoring** - Track device health status over time
- **Custom queries** - Create your own visualizations

### Deployment
- **Docker Compose** - Single command deployment
- **Service orchestration** - Zookeeper, Kafka, TimescaleDB, and Grafana
- **Health checks** - Automatic service dependency management
- **Volume persistence** - Data survives container restarts
- **Environment configuration** - Easy setup via .env file

## Quick Start

### Prerequisites
- Docker and Docker Compose installed
- Access to Dell OpenManage Enterprise with Kafka integration
- Network connectivity to OME Kafka brokers

### Setup

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd ome-kafka-telemetry-grafana
   ```

2. **Create environment configuration**
   ```bash
   cp .env.example .env
   ```

3. **Configure environment variables**
   
   Edit `.env` and set your configuration:
   ```bash
   # Kafka Configuration
   KAFKA_BOOTSTRAP_SERVERS=your-ome-kafka-server:9092
   KAFKA_TELEMETRY_TOPIC=ome/telemetry
   KAFKA_ALERT_TOPIC=ome/alerts
   KAFKA_HEALTH_TOPIC=ome/health
   KAFKA_GROUP_ID=ome-consumer-group
   
   # TimescaleDB Configuration
   POSTGRES_DB=ometelemetry
   POSTGRES_USER=omeuser
   POSTGRES_PASSWORD=your-secure-password
   TIMESCALEDB_HOST=timescaledb
   TIMESCALEDB_PORT=5432
   
   # Grafana Configuration
   GF_SECURITY_ADMIN_USER=admin
   GF_SECURITY_ADMIN_PASSWORD=your-admin-password
   GF_INSTALL_PLUGINS=
   ```

4. **Start the application**
   ```bash
   docker-compose up -d
   ```

5. **Verify services are running**
   ```bash
   docker-compose ps
   ```
   
   All services should show as "healthy" or "running".

6. **Access Grafana**
   
   Open your browser to: `http://localhost:3000`
   
   - Username: `admin` (or your configured value)
   - Password: Your configured password
   
   The OME Telemetry dashboard should be automatically provisioned.

### Viewing Data

Once the application is running:

1. **Monitor logs**
   ```bash
   # View all logs
   docker-compose logs -f
   
   # View specific service
   docker-compose logs -f ome-app
   ```

2. **Check TimescaleDB**
   ```bash
   docker exec -it timescaledb psql -U omeuser -d ometelemetry
   ```
   
   Query metrics:
   ```sql
   SELECT * FROM metrics ORDER BY time DESC LIMIT 10;
   SELECT * FROM alerts ORDER BY time DESC LIMIT 10;
   SELECT * FROM devices;
   ```

3. **Grafana Dashboard**
   - Navigate to Dashboards → OME Telemetry
   - View real-time metrics, alerts, and health status
   - Customize time ranges and queries as needed

### Stopping the Application

```bash
# Stop services
docker-compose down

# Stop and remove volumes (deletes data)
docker-compose down -v
```

## Configuration

See [config.py](config.py) for all available configuration options. Key settings include:

- **Kafka Topics**: Configure which topics to consume
- **Consumer Settings**: Group ID, offset reset behavior
- **Database Settings**: Connection parameters, pool size
- **Logging**: Log level and format configuration

## Development

### Running locally
```bash
# Install dependencies
pip install -r requirements.txt

# Run the application
python main.py
```

## Troubleshooting

### Kafka Connection Issues
- Verify `KAFKA_BOOTSTRAP_SERVERS` is correct
- Ensure network connectivity to Kafka brokers
- Check Kafka topic names match OME configuration

### Database Connection Issues
- Verify TimescaleDB is healthy: `docker-compose ps`
- Check database credentials in `.env`
- Review logs: `docker-compose logs timescaledb`

### No Data in Grafana
- Confirm OME is publishing to Kafka topics
- Check consumer logs: `docker-compose logs ome-app`
- Verify data exists in TimescaleDB (see querying section above)

## License

[Add your license information here]

## Support

[Add support contact information here]
