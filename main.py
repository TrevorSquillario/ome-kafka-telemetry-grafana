"""Main application entry point for OME Kafka Telemetry to TimescaleDB."""
import logging
import signal
import sys
from config import settings
from services.stream_processor import OMEDataRouter
from services.timescaledb_service import TimescaleDBService
from services import ome_helper

# Configure centralized logging
from logging_config import configure_logging

configure_logging(level=getattr(logging, settings.log_level.upper()))
logger = logging.getLogger(__name__)

# Global references for cleanup
router = None
db_service = None


def handle_telemetry(data: dict):
    """Handle telemetry messages from Kafka.
    
    Args:
        data: Telemetry data from OME Kafka topic
    """
    try:
        # Parse telemetry data into individual metrics
        metrics = ome_helper.parse_telemetry_data(data)
        
        if metrics:
            # Insert metrics into TimescaleDB
            db_service.insert_metrics(metrics)
            logger.info(f"Processed {len(metrics)} telemetry metrics from device {data.get('Identifier')}")
        else:
            logger.warning(f"No valid metrics found in telemetry data")
            
    except Exception as e:
        logger.error(f"Error handling telemetry: {e}", exc_info=True)


def handle_alert(data: dict):
    """Handle alert messages from Kafka.
    
    Args:
        data: Alert data from OME Kafka topic
    """
    try:
        # Normalize alert data
        normalized = ome_helper.normalize_alert_data(data)
        
        if normalized:
            # Prepare alert for database
            alert = {
                'time': normalized.get('time') if 'time' in normalized else None,
                'device_id': normalized.get('device_id') or normalized.get('system_service_tag'),
                'alert_id': normalized.get('alert_identifier') or normalized.get('alertid'),
                'severity': normalized.get('severity', 'UNKNOWN'),
                'message': normalized.get('message') or normalized.get('description'),
                'category': normalized.get('category') or normalized.get('message_id'),
                'details': normalized
            }
            
            # Insert alert into TimescaleDB
            db_service.insert_alert(alert)
            logger.info(f"Processed alert: {alert['alert_id']} - {alert['severity']}")
        else:
            logger.warning("No valid alert data found")
            
    except Exception as e:
        logger.error(f"Error handling alert: {e}", exc_info=True)


def handle_health(data: dict):
    """Handle health messages from Kafka.
    
    Args:
        data: Health data from OME Kafka topic
    """
    try:
        # Normalize health data
        normalized = ome_helper.normalize_health_data(data)
        
        if normalized and normalized.get('device_id'):
            # Insert health into TimescaleDB
            db_service.insert_health(normalized)
            logger.info(f"Processed health status for device: {normalized['device_id']}")
        else:
            logger.warning("No valid health data found")
            
    except Exception as e:
        logger.error(f"Error handling health: {e}", exc_info=True)


def signal_handler(sig, frame):
    """Handle shutdown signals gracefully."""
    logger.info("Shutdown signal received, cleaning up...")
    if router:
        router.stop()
    if db_service:
        db_service.close()
    sys.exit(0)


def main():
    """Main application entry point."""
    global router, db_service
    
    logger.info("Starting OME Kafka Telemetry Application")
    logger.info(f"Kafka Bootstrap Servers: {settings.kafka_bootstrap_servers}")
    logger.info(f"TimescaleDB Host: {settings.timescaledb_host}")
    
    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        # Initialize TimescaleDB service
        logger.info("Connecting to TimescaleDB...")
        db_service = TimescaleDBService(
            host=settings.timescaledb_host,
            port=settings.timescaledb_port,
            database=settings.timescaledb_database,
            user=settings.timescaledb_user,
            password=settings.timescaledb_password
        )
        db_service.connect()
        logger.info("Successfully connected to TimescaleDB")
        
        # Initialize the OMEDataRouter with callbacks that write to TimescaleDB.
        logger.info("Initializing OME data router...")
        router = OMEDataRouter(
            timescaledb_service=db_service,
        )

        # Start consuming messages (router will read topics from settings)
        logger.info("Starting OME Data Router (Kafka consumer)")
        router.start()
        
    except KeyboardInterrupt:
        logger.info("Application interrupted by user")
    except Exception as e:
        logger.error(f"Application error: {e}", exc_info=True)
        sys.exit(1)
    finally:
        # Cleanup
        if router:
            router.stop()
        if db_service:
            db_service.close()
        logger.info("Application shutdown complete")


if __name__ == '__main__':
    main()
