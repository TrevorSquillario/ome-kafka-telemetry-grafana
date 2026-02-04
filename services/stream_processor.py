"""Kafka Stream Processor Service for OME data ingestion."""
import json
import logging
from typing import Dict, Any, Callable, Optional
from .ome_helper import parse_telemetry_data
from confluent_kafka import Consumer, KafkaError, KafkaException
from config import settings, SEVERITY_NAME_TO_VALUE

logger = logging.getLogger(__name__)


class KafkaStreamProcessor:
    """Processes Kafka streams from OpenManage Enterprise."""
    
    def __init__(self):
        """Initialize Kafka consumer with configuration."""
        self.config = {
            'bootstrap.servers': settings.kafka_bootstrap_servers,
            'group.id': settings.kafka_group_id,
            'auto.offset.reset': settings.kafka_auto_offset_reset,
            'enable.auto.commit': True,
            'session.timeout.ms': 6000
        }
        self.consumer = None
        self.running = False
        self.handlers: Dict[str, Callable] = {}
        
    def register_handler(self, topic: str, handler: Callable[[Dict[str, Any]], None]):
        """Register a handler function for a specific topic.
        
        Args:
            topic: Kafka topic name
            handler: Callback function to process messages
        """
        self.handlers[topic] = handler
        logger.info(f"Registered handler for topic: {topic}")
        
    def start(self, topics: list[str]):
        """Start consuming messages from specified topics.
        
        Args:
            topics: List of Kafka topics to subscribe to
        """
        try:
            self.consumer = Consumer(self.config)
            self.consumer.subscribe(topics)
            self.running = True
            logger.info(f"Kafka consumer started. Subscribed to topics: {topics}")
            
            while self.running:
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    continue
                    
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.debug(f"Reached end of partition: {msg.topic()} [{msg.partition()}]")
                    else:
                        raise KafkaException(msg.error())
                else:
                    self._process_message(msg)
                    
        except KeyboardInterrupt:
            logger.info("Kafka consumer interrupted by user")
        except Exception as e:
            logger.error(f"Error in Kafka consumer: {e}", exc_info=True)
        finally:
            self.stop()
            
    def _process_message(self, msg):
        """Process a single Kafka message.
        
        Args:
            msg: Kafka message object
        """
        try:
            topic = msg.topic()
            value = msg.value().decode('utf-8')
            data = json.loads(value)
            
            logger.debug(f"Received message from topic {topic}: {data}")
            
            # Route to appropriate handler
            if topic in self.handlers:
                self.handlers[topic](data)
            else:
                logger.warning(f"No handler registered for topic: {topic}")
                
        except json.JSONDecodeError as e:
            logger.error(f"Failed to decode JSON message: {e}")
        except Exception as e:
            logger.error(f"Error processing message: {e}", exc_info=True)
            
    def stop(self):
        """Stop the Kafka consumer."""
        self.running = False
        if self.consumer:
            self.consumer.close()
            logger.info("Kafka consumer stopped")


class OMEDataRouter:
    """Routes OME Kafka messages to appropriate processing services."""
    
    def __init__(
        self,
        timescaledb_service: Optional[Any] = None,
    ):
        """Initialize router with optional ML/LLM engines and callbacks.

        If callbacks are provided they will be used for processing messages;
        otherwise the router falls back to the ml_engine/llm_engine usage.
        """
        self.processor = KafkaStreamProcessor()
        self.timescaledb_service = timescaledb_service

        # Register handlers for each topic present in the comma-separated
        # `settings.kafka_topics`. This allows flexible topic names/prefixes
        # (for example: "ome.telemetry,ome.alerts,ome.health").
        topics = [t.strip() for t in settings.kafka_topics.split(',') if t.strip()]
        for topic in topics:
            t = topic.lower()
            if 'inventory' in t:
                self.processor.register_handler(topic, self.handle_inventory)
            elif 'health' in t:
                self.processor.register_handler(topic, self.handle_health)
            elif 'alert' in t or 'alerts' in t:
                self.processor.register_handler(topic, self.handle_alerts)
            elif 'telemetry' in t:
                self.processor.register_handler(topic, self.handle_telemetry)
            elif 'audit' in t:
                self.processor.register_handler(topic, self.handle_audit)
            else:
                logger.warning(f"No local handler implemented for topic: {topic}")
        
    def handle_inventory(self, data: Dict[str, Any]):
        """Handle inventory data messages.
        
        Args:
            data: Inventory data from OME
        """
        # If an external callback is provided, use it (simpler integration).
        if self.inventory_cb:
            return self.inventory_cb(data)

        logger.info(f"Processing inventory data: {len(data)} items")
        # Extract numerical metrics and send to ML engine if available
        if 'devices' in data and self.ml_engine:
            for device in data['devices']:
                if 'metrics' in device:
                    self.ml_engine.process_metrics(device['metrics'])
                    
    def handle_health(self, data: Dict[str, Any]):
        """Handle health data messages.
        
        Args:
            data: Health data from OME
        """
        # Use callback if provided
        if self.health_cb:
            return self.health_cb(data)

        logger.info("Processing health data")
        # Process health metrics with ML engine if available
        if 'health_metrics' in data and self.ml_engine:
            self.ml_engine.process_health(data['health_metrics'])
            
    def handle_alerts(self, data: Dict[str, Any]):
        """Handle alert messages.
        
        Args:
            data: Alert data from OME
        """
        logger.info("Processing alert payload")
        try:
            # Support payloads where alerts are nested under a top-level 'Data' list
            records = None
            if isinstance(data.get('Data'), list):
                records = data.get('Data')
            elif isinstance(data, dict) and any(k.lower() in ('severity', 'description', 'message') for k in data.keys()):
                # Already a single alert dict
                records = [data]
            else:
                # Fallback: try to treat the whole payload as a single record
                records = [data]

            for rec in records:
                severity = rec.get('Severity') or rec.get('severity') or 'UNKNOWN'
                identifier = rec.get('AlertIdentifier') or rec.get('alertIdentifier') or 'N/A'
                message_id = rec.get('EEMIMessageId') or rec.get('eemimessageid') or 'N/A'
                logger.debug(f"Processing alert: {severity}, Identifier: {identifier}, Message ID: {message_id}")

                # Prefer Description, then Message, then other text fields
                alert_text = (
                    rec.get('Description')
                    or rec.get('description')
                    or rec.get('Message')
                    or rec.get('message')
                )

                # If no text field, serialize the record for LLM analysis
                if not alert_text:
                    try:
                        alert_text = json.dumps(rec)
                    except Exception:
                        alert_text = str(rec)

                # Determine numeric severity and compare against configured threshold
                try:
                    raw_sev = rec.get('Severity') or rec.get('severity')
                    if raw_sev is None:
                        sev_num = 1
                    else:
                        try:
                            sev_num = int(raw_sev)
                        except Exception:
                            # Fallback mapping from textual severity names
                            sev_name = str(raw_sev).strip().lower()
                            # Use central mapping from config
                            sev_num = SEVERITY_NAME_TO_VALUE.get(sev_name, 1)
                except Exception:
                    sev_num = 1

                # If an external alert callback is provided, hand off the raw
                # record to it; otherwise fall back to LLM analysis if present.
                if self.alert_cb:
                    self.alert_cb(rec)
                else:
                    if sev_num >= settings.alert_min_severity and self.llm_engine:
                        self.llm_engine.analyze_alert(alert_text, rec)
                    else:
                        logger.debug(
                            f"Alert severity {sev_num} below threshold {settings.alert_min_severity} or no LLM available; skipping LLM analysis"
                        )
        except Exception as e:
            logger.error(f"Error handling alerts: {e}", exc_info=True)
            
    def handle_telemetry(self, data: Dict[str, Any]):
        """Handle telemetry data messages.
        
        Args:
            data: Telemetry data from OME
        """
        # If a TimescaleDB service is configured, parse and insert metrics.
        if self.timescaledb_service:
            try:
                metrics = parse_telemetry_data(data)
                if metrics:
                    self.timescaledb_service.insert_metrics(metrics)
                    logger.info(f"Inserted {len(metrics)} telemetry metrics into TimescaleDB")
                    return
            except Exception as e:
                logger.error(f"Failed inserting telemetry into TimescaleDB: {e}", exc_info=True)
            
    def handle_audit(self, data: Dict[str, Any]):
        """Handle audit log messages.
        
        Args:
            data: Audit log data from OME
        """
        # Use external audit callback when provided
        if self.audit_cb:
            return self.audit_cb(data)

        logger.info("Processing audit log")
        # Send audit logs to LLM for analysis
        if 'log_message' in data or 'action' in data:
            log_text = f"{data.get('action', '')}: {data.get('log_message', '')}"
            if self.llm_engine:
                self.llm_engine.analyze_audit_log(log_text, data)
            
    def start(self):
        """Start the data router and begin consuming messages."""
        # Use the configured comma-separated topics string to subscribe.
        topics = [t.strip() for t in settings.kafka_topics.split(',') if t.strip()]
        logger.info("Starting OME Data Router")
        self.processor.start(topics)
        
    def stop(self):
        """Stop the data router."""
        logger.info("Stopping OME Data Router")
        self.processor.stop()
