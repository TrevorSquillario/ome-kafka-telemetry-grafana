"""Configuration settings for the OME Kafka Telemetry application."""
import os
from pydantic_settings import BaseSettings
from pydantic import ConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""
    
    # Kafka Configuration
    kafka_bootstrap_servers: str = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    kafka_group_id: str = os.getenv('KAFKA_GROUP_ID', 'ome-telemetry-consumer')
    kafka_auto_offset_reset: str = os.getenv('KAFKA_AUTO_OFFSET_RESET', 'earliest')
    kafka_topics: str = os.getenv('KAFKA_TOPICS', 'ome.telemetry,ome.alerts,ome.health')
    
    # TimescaleDB Configuration
    timescaledb_host: str = os.getenv('TIMESCALEDB_HOST', 'timescaledb')
    timescaledb_port: int = int(os.getenv('TIMESCALEDB_PORT', '5432'))
    timescaledb_database: str = os.getenv('TIMESCALEDB_DATABASE', 'ome_telemetry')
    timescaledb_user: str = os.getenv('TIMESCALEDB_USER', 'postgres')
    timescaledb_password: str = os.getenv('TIMESCALEDB_PASSWORD', 'postgres')
    
    # Application Configuration
    log_level: str = os.getenv('LOG_LEVEL', 'INFO')
    
    # Pydantic v2 configuration: allow unknown env vars (ignore extras)
    model_config = ConfigDict(
        env_file='.env',
        case_sensitive=False,
        extra='ignore',
    )


settings = Settings()

# Severity mapping for alerts
SEVERITY_NAME_TO_VALUE = {
    'unknown': 1,
    'info': 2,
    'normal': 4,
    'warning': 8,
    'critical': 16,
}

SEVERITY_VALUE_TO_NAME = {v: k for k, v in SEVERITY_NAME_TO_VALUE.items()}
