"""TimescaleDB Service for storing time-series metrics."""
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_values, Json
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import threading
import time

logger = logging.getLogger(__name__)


class TimescaleDBService:
    """Service for interacting with TimescaleDB."""
    
    def __init__(self, host: str, port: int, database: str, user: str, password: str):
        """Initialize TimescaleDB connection.
        
        Args:
            host: Database host
            port: Database port
            database: Database name
            user: Database user
            password: Database password
        """
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.connection = None
        # Background periodic metrics fetch
        self._stop_event = threading.Event()
        self._metrics_thread = threading.Thread(
            target=self._periodic_recent_metrics, daemon=True
        )
        self._metrics_thread.start()
        
    def connect(self):
        """Establish connection to TimescaleDB and create necessary tables."""
        try:
            # First connect to postgres database to create our database if needed
            conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                database='postgres',
                user=self.user,
                password=self.password
            )
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cursor = conn.cursor()
            
            # Create database if it doesn't exist
            cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{self.database}'")
            if not cursor.fetchone():
                cursor.execute(f"CREATE DATABASE {self.database}")
                logger.info(f"Created database: {self.database}")
            
            cursor.close()
            conn.close()
            
            # Now connect to our target database
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password
            )
            logger.info(f"Connected to TimescaleDB at {self.host}:{self.port}/{self.database}")
            
            # Create tables and hypertables
            self._create_tables()
            
        except Exception as e:
            logger.error(f"Failed to connect to TimescaleDB: {e}", exc_info=True)
            raise
            
    def _create_tables(self):
        """Create necessary tables and hypertables."""
        try:
            cursor = self.connection.cursor()
            
            # Enable TimescaleDB extension
            cursor.execute("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;")
            
            # Create metrics table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS metrics (
                    time TIMESTAMPTZ NOT NULL,
                    device_id TEXT NOT NULL,
                    metric_id TEXT NOT NULL,
                    component_id TEXT,
                    value DOUBLE PRECISION,
                    tags JSONB
                );
            """)
            
            # Convert to hypertable if not already
            cursor.execute("""
                SELECT 1 FROM timescaledb_information.hypertables 
                WHERE hypertable_name = 'metrics';
            """)
            if not cursor.fetchone():
                cursor.execute("""
                    SELECT create_hypertable('metrics', 'time', 
                                            if_not_exists => TRUE,
                                            migrate_data => TRUE);
                """)
                logger.info("Created hypertable: metrics")
            
            # Create indexes for better query performance
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_metrics_device_id 
                ON metrics (device_id, time DESC);
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_metrics_metric_id 
                ON metrics (metric_id, time DESC);
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_metrics_component_id 
                ON metrics (component_id, time DESC) WHERE component_id IS NOT NULL;
            """)
            
            # Create alerts table for storing alert events
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS alerts (
                    time TIMESTAMPTZ NOT NULL,
                    device_id TEXT,
                    alert_id TEXT,
                    severity TEXT,
                    message TEXT,
                    category TEXT,
                    details JSONB
                );
            """)
            
            # Convert alerts to hypertable
            cursor.execute("""
                SELECT 1 FROM timescaledb_information.hypertables 
                WHERE hypertable_name = 'alerts';
            """)
            if not cursor.fetchone():
                cursor.execute("""
                    SELECT create_hypertable('alerts', 'time',
                                            if_not_exists => TRUE,
                                            migrate_data => TRUE);
                """)
                logger.info("Created hypertable: alerts")
            
            # Create health table for device health status
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS health (
                    time TIMESTAMPTZ NOT NULL,
                    device_id TEXT NOT NULL,
                    health_status TEXT,
                    health_value INTEGER,
                    details JSONB
                );
            """)
            
            # Convert health to hypertable
            cursor.execute("""
                SELECT 1 FROM timescaledb_information.hypertables 
                WHERE hypertable_name = 'health';
            """)
            if not cursor.fetchone():
                cursor.execute("""
                    SELECT create_hypertable('health', 'time',
                                            if_not_exists => TRUE,
                                            migrate_data => TRUE);
                """)
                logger.info("Created hypertable: health")
            
            self.connection.commit()
            cursor.close()
            logger.info("Successfully created/verified all tables and hypertables")
            
        except Exception as e:
            logger.error(f"Failed to create tables: {e}", exc_info=True)
            self.connection.rollback()
            raise
            
    def insert_metrics(self, metrics: List[Dict[str, Any]]):
        """Insert multiple metric data points.
        
        Args:
            metrics: List of metric dictionaries with keys:
                    - time: datetime object
                    - device_id: str
                    - metric_id: str
                    - component_id: Optional[str]
                    - value: float
                    - tags: Optional[Dict]
        """
        if not metrics:
            return
            
        try:
            cursor = self.connection.cursor()
            
            values = [
                (
                    m['time'],
                    m['device_id'],
                    m['metric_id'],
                    m.get('component_id'),
                    m['value'],
                    Json(m.get('tags')) if m.get('tags') is not None else None
                )
                for m in metrics
            ]
            
            execute_values(
                cursor,
                """
                INSERT INTO metrics (time, device_id, metric_id, component_id, value, tags)
                VALUES %s
                """,
                values
            )
            
            self.connection.commit()
            cursor.close()
            logger.debug(f"Inserted {len(metrics)} metrics into TimescaleDB")
            
        except Exception as e:
            logger.error(f"Failed to insert metrics: {e}", exc_info=True)
            self.connection.rollback()
            raise
            
    def insert_alert(self, alert: Dict[str, Any]):
        """Insert an alert record.
        
        Args:
            alert: Alert dictionary with keys:
                  - time: datetime object
                  - device_id: str
                  - alert_id: str
                  - severity: str
                  - message: str
                  - category: str
                  - details: Dict
        """
        try:
            cursor = self.connection.cursor()
            
            cursor.execute(
                """
                INSERT INTO alerts (time, device_id, alert_id, severity, message, category, details)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    alert.get('time', datetime.now()),
                    alert.get('device_id'),
                    alert.get('alert_id'),
                    alert.get('severity'),
                    alert.get('message'),
                    alert.get('category'),
                    Json(alert.get('details')) if alert.get('details') is not None else None
                )
            )
            
            self.connection.commit()
            cursor.close()
            logger.debug(f"Inserted alert: {alert.get('alert_id')}")
            
        except Exception as e:
            logger.error(f"Failed to insert alert: {e}", exc_info=True)
            self.connection.rollback()
            raise
            
    def insert_health(self, health: Dict[str, Any]):
        """Insert a health status record.
        
        Args:
            health: Health dictionary with keys:
                   - time: datetime object
                   - device_id: str
                   - health_status: str
                   - health_value: int
                   - details: Dict
        """
        try:
            cursor = self.connection.cursor()
            
            cursor.execute(
                """
                INSERT INTO health (time, device_id, health_status, health_value, details)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (
                    health.get('time', datetime.now()),
                    health['device_id'],
                    health.get('health_status'),
                    health.get('health_value'),
                    Json(health.get('details')) if health.get('details') is not None else None
                )
            )
            
            self.connection.commit()
            cursor.close()
            logger.debug(f"Inserted health status for device: {health['device_id']}")
            
        except Exception as e:
            logger.error(f"Failed to insert health: {e}", exc_info=True)
            self.connection.rollback()
            raise
            
    def close(self):
        """Close the database connection."""
        # Stop periodic thread
        try:
            self._stop_event.set()
            if self._metrics_thread.is_alive():
                self._metrics_thread.join(timeout=2)
        except Exception:
            pass

        if self.connection:
            self.connection.close()
            logger.info("Closed TimescaleDB connection")

    def _periodic_recent_metrics(self):
        """Background thread that calls `get_recent_metrics` every 30 seconds.

        It waits until a DB connection is available, then repeatedly fetches
        recent metrics and logs a small table to the debug logger. Stops when
        `self._stop_event` is set.
        """
        # Wait until connection exists or stop requested
        while not self._stop_event.is_set():
            if self.connection:
                try:
                    self.get_recent_metrics()
                except Exception:
                    logger.debug("Periodic get_recent_metrics failed", exc_info=True)

            # sleep in small intervals so we can exit promptly
            for _ in range(30):
                if self._stop_event.wait(timeout=1):
                    break

    def get_recent_metrics(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Fetch recent metrics and log a table to debug.

        Args:
            limit: Maximum number of rows to fetch (most recent first).

        Returns:
            A list of metric dictionaries fetched from the database.
        """
        metrics: List[Dict[str, Any]] = []
        if not self.connection:
            logger.debug("get_recent_metrics: no DB connection available yet")
            return metrics

        try:
            cursor = self.connection.cursor()
            cursor.execute(
                """
                SELECT time, device_id, metric_id, value
                FROM metrics
                ORDER BY time DESC
                LIMIT %s
                """,
                (limit,)
            )
            rows = cursor.fetchall()
            cursor.close()

            # Build list of dicts
            for r in rows:
                metrics.append({
                    "time": r[0],
                    "device_id": r[1],
                    "metric_id": r[2],
                    "value": r[3],
                })

            # Format simple table
            if metrics:
                headers = ["time", "device_id", "metric_id", "value"]
                # Determine column widths
                cols = [[str(m[h]) for m in metrics] for h in headers]
                col_widths = [max(len(h), max((len(v) for v in col), default=0)) for h, col in zip(headers, cols)]

                sep = "+" + "+".join("-" * (w + 2) for w in col_widths) + "+"
                header_row = "| " + " | ".join(h.ljust(w) for h, w in zip(headers, col_widths)) + " |"

                lines = [sep, header_row, sep]
                for m in metrics:
                    row = "| " + " | ".join(str(m[h]).ljust(w) for h, w in zip(headers, col_widths)) + " |"
                    lines.append(row)
                lines.append(sep)

                table_str = "\n".join(lines)
                logger.debug("Recent metrics:\n%s", table_str)
            else:
                logger.debug("No recent metrics found")

            return metrics

        except Exception as e:
            logger.error(f"Failed to fetch recent metrics: {e}", exc_info=True)
            return metrics
