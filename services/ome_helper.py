"""Helper utilities for normalizing OME alert data and extracting device info."""
import re
from typing import Dict, Any, List
from datetime import datetime
from config import SEVERITY_VALUE_TO_NAME


def parse_description(desc: str) -> Dict[str, Any]:
    """Extract useful bits from freeform description text."""
    out: Dict[str, Any] = {}

    m = re.search(r"System Service Tag:\s*([A-Za-z0-9_-]+)", desc)
    if m:
        out['system_service_tag'] = m.group(1)

    m = re.search(r"Device Display Name:\s*([^,]+)", desc)
    if m:
        out['device_display_name'] = m.group(1).strip()

    m = re.search(r"RAC FQDN:\s*([^,\n]+)", desc)
    if m:
        out['rac_fqdn'] = m.group(1).strip()

    m = re.search(r"Message ID:\s*([^,\n]+)", desc)
    if m:
        out['message_id'] = m.group(1).strip()

    return out


def normalize_alert_data(data: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize incoming alert dicts to a consistent shape.

    Handles containers with 'Data' lists and capitalized keys. Returns a
    dict with lowercased/underscored keys and some parsed fields.
    """
    # If container with 'Data' list, pick first entry
    if isinstance(data, dict) and 'Data' in data and isinstance(data['Data'], list) and data['Data']:
        data = data['Data'][0]

    normalized: Dict[str, Any] = {}
    if not isinstance(data, dict):
        return normalized

    for k, v in data.items():
        nk = k.strip().lower().replace(' ', '_')
        normalized[nk] = v

    # Map known variants
    if 'severity' in normalized:
        try:
            sev = int(normalized['severity'])
            normalized['severity'] = SEVERITY_VALUE_TO_NAME.get(sev, str(sev))
        except Exception:
            normalized['severity'] = normalized.get('severity', 'UNKNOWN')

    # If description available, try to parse and pull out service tag, fqdn, etc.
    if 'description' in normalized and normalized.get('description'):
        parsed = parse_description(str(normalized['description']))
        normalized.update(parsed)

    # Normalize alert id/name fields
    if 'alertidentifier' in normalized and 'alert_identifier' not in normalized:
        normalized['alert_identifier'] = normalized.get('alertidentifier')

    return normalized


def extract_device_info(data: Dict[str, Any]) -> str:
    """Extract device information from alert data and format a summary string."""
    parts: List[str] = []

    alert_id = data.get('alert_identifier') or data.get('alertid')
    svc_tag = data.get('system_service_tag') or data.get('systemservicetag')
    if svc_tag:
        parts.append(f"ServiceTag:{svc_tag}")
    elif alert_id:
        parts.append(str(alert_id))

    dname = data.get('device_display_name') or data.get('device_name') or data.get('system_name')
    if dname:
        parts.append(str(dname))

    fqdn = data.get('rac_fqdn')
    if fqdn:
        parts.append(str(fqdn))

    device_id = data.get('device_id')
    if not parts and device_id:
        parts.append(str(device_id))

    if not parts:
        return 'unknown'

    return ' | '.join(parts)


def parse_telemetry_data(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Parse OME telemetry data into individual metric records.
    
    OME telemetry data comes in the format:
    {
        "Identifier": "device_service_tag",
        "Metric": [
            {
                "MetricId": "PSU.AmpsReading.Average.5.Interval",
                "TimeStamp": ["20260201T183500Z", "20260201T183000Z"],
                "ComponentId": "PSU.Slot.1",
                "MetricValue": ["0.8", "0.8"]
            },
            ...
        ]
    }
    
    Args:
        data: Telemetry data from Kafka message
        
    Returns:
        List of metric dictionaries ready for TimescaleDB insertion
    """
    metrics = []

    # Support payloads that are lists of telemetry containers
    if isinstance(data, list):
        for item in data:
            metrics.extend(parse_telemetry_data(item))
        return metrics

    # Support envelope with 'Data' list (common in other messages)
    if isinstance(data, dict) and 'Data' in data and isinstance(data['Data'], list):
        for item in data['Data']:
            metrics.extend(parse_telemetry_data(item))
        return metrics

    # Extract device identifier (service tag) - accept different casings
    device_id = data.get('Identifier') or data.get('identifier') or 'unknown'
    
    # Process each metric in the Metric array
    metric_list = data.get('Metric', [])
    if not isinstance(metric_list, list):
        return metrics
    
    for metric in metric_list:
        metric_id = metric.get('MetricId')
        component_id = metric.get('ComponentId')
        timestamps = metric.get('TimeStamp', [])
        values = metric.get('MetricValue', [])
        
        # Ensure timestamps and values are lists
        if not isinstance(timestamps, list):
            timestamps = [timestamps]
        if not isinstance(values, list):
            values = [values]
        
        # Create a metric entry for each timestamp/value pair
        for timestamp, value in zip(timestamps, values):
            try:
                # Parse OME timestamp format: "20260201T183500Z"
                time = parse_ome_timestamp(timestamp)
                
                # Convert value to float
                metric_value = float(value)
                
                # Extract metric metadata from MetricId
                tags = parse_metric_id(metric_id)
                
                metrics.append({
                    'time': time,
                    'device_id': device_id,
                    'metric_id': metric_id,
                    'component_id': component_id,
                    'value': metric_value,
                    'tags': tags
                })
                
            except (ValueError, TypeError) as e:
                # Skip invalid metric entries
                continue
    
    return metrics


def parse_ome_timestamp(timestamp: str) -> datetime:
    """Parse OME timestamp format into datetime object.
    
    OME uses format: "20260201T183500Z" (YYYYMMDDTHHMMSSsZ)
    
    Args:
        timestamp: Timestamp string from OME
        
    Returns:
        datetime object
    """
    # Remove 'Z' suffix and parse
    timestamp_clean = timestamp.rstrip('Z')
    return datetime.strptime(timestamp_clean, '%Y%m%dT%H%M%S')


def parse_metric_id(metric_id: str) -> Dict[str, str]:
    """Parse MetricId to extract metric metadata.
    
    MetricId format examples:
    - "PSU.AmpsReading.Average.5.Interval"
    - "PMP_CPU.TemperatureReading.Min.5.Interval"
    - "Grid_A.AmpsReading.Maximum.5.Interval"
    
    Args:
        metric_id: MetricId string from OME
        
    Returns:
        Dictionary with parsed metadata
    """
    tags = {}
    
    if not metric_id:
        return tags
    
    parts = metric_id.split('.')
    
    if len(parts) >= 2:
        # Component type (e.g., PSU, PMP_CPU, Grid_A)
        tags['component_type'] = parts[0]
        
        # Metric type (e.g., AmpsReading, TemperatureReading)
        tags['metric_type'] = parts[1]
    
    if len(parts) >= 3:
        # Aggregation type (e.g., Average, Min, Max)
        tags['aggregation'] = parts[2]
    
    if len(parts) >= 4:
        # Interval duration
        tags['interval'] = parts[3]
    
    # Extract unit from metric type
    metric_type = tags.get('metric_type', '')
    if 'Amps' in metric_type:
        tags['unit'] = 'amperes'
    elif 'Temperature' in metric_type:
        tags['unit'] = 'celsius'
    elif 'Power' in metric_type or 'Energy' in metric_type:
        tags['unit'] = 'watts'
    elif 'Voltage' in metric_type:
        tags['unit'] = 'volts'
    
    return tags


def normalize_health_data(data: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize OME health data for storage.
    
    Args:
        data: Health data from Kafka message
        
    Returns:
        Normalized health dictionary
    """
    # Handle container with 'Data' list
    if isinstance(data, dict) and 'Data' in data and isinstance(data['Data'], list) and data['Data']:
        data = data['Data'][0]
    
    normalized = {}
    if not isinstance(data, dict):
        return normalized
    
    # Lowercase and underscore keys
    for k, v in data.items():
        nk = k.strip().lower().replace(' ', '_')
        normalized[nk] = v
    
    # Extract device ID
    normalized['device_id'] = (
        normalized.get('device_id') or 
        normalized.get('deviceid') or
        normalized.get('id') or
        'unknown'
    )
    
    # Extract health status
    normalized['health_status'] = (
        normalized.get('health_status') or
        normalized.get('healthstatus') or
        normalized.get('status') or
        'UNKNOWN'
    )
    
    # Convert health to numeric value if needed
    health_mapping = {
        'HEALTHY': 1000,
        'WARNING': 2000,
        'CRITICAL': 3000,
        'UNKNOWN': 0
    }
    
    health_status = str(normalized['health_status']).upper()
    normalized['health_value'] = health_mapping.get(health_status, 0)
    
    # Add timestamp
    normalized['time'] = datetime.now()
    
    return normalized
