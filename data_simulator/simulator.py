### old simulator 
# import time
# import json
# import random
# from datetime import datetime, timedelta, date
# from kafka import KafkaProducer
# from sensors import UltrasonicSensor, TemperatureSensor, GPSSensor, CameraSensor
# from data_generators import generate_bin_data, generate_citizen_report

# class WasteBinSimulator:
#     def __init__(self, kafka_brokers='localhost:9092'):
#         self.bins = generate_bin_data(100)
#         self.last_capture = {}
#         self.sensors = self._initialize_sensors()
#         self.camera_interval = timedelta(minutes=15)

#         self.topics = {
#             "bins": "bin-metadata",
#             "sensor": "waste-sensor-data",
#             "reports": "citizen-reports",
#             "maintenance": "maintenance-events"
#         }

#         print(f"Connecting to Kafka at {kafka_brokers}...")

#         # JSON serializer to handle datetime/date
#         def json_serializer(obj):
#             if isinstance(obj, (datetime, date)):
#                 return obj.isoformat()
#             return str(obj)

#         self.producer = KafkaProducer(
#             bootstrap_servers=kafka_brokers,
#             value_serializer=lambda v: json.dumps(v, default=json_serializer).encode('utf-8'),
#             retries=5
#         )
#         print("Connected to Kafka!")

#     def _initialize_sensors(self):
#         sensors = {}
#         for bin in self.bins:
#             bin_id = bin['bin_id']
#             lat = bin['location']['coordinates']['lat']
#             lon = bin['location']['coordinates']['lon']
#             capacity = bin['capacity_kg']

#             sensors[bin_id] = {
#                 "ultrasonic": UltrasonicSensor(bin_id, capacity),
#                 "temperature": TemperatureSensor(bin_id),
#                 "gps": GPSSensor(bin_id, lat, lon),
#                 "camera": CameraSensor(bin_id)
#             }
#             self.last_capture[bin_id] = datetime.now()
#         return sensors

#     def _generate_sensor_readings(self, bin_id):
#         ultrasonic = self.sensors[bin_id]["ultrasonic"].measure()
#         fill_level = ultrasonic.value
#         capacity = next(b["capacity_kg"] for b in self.bins if b["bin_id"] == bin_id)
#         weight = round((fill_level / 100) * capacity, 1)

#         readings = [
#             ultrasonic.__dict__,
#             self.sensors[bin_id]["temperature"].measure(fill_level).__dict__,
#             self.sensors[bin_id]["gps"].measure().__dict__,
#             {
#                 "bin_id": bin_id,
#                 "timestamp": datetime.utcnow(),
#                 "sensor_type": "weight",
#                 "value": weight,
#                 "unit": "kg"
#             },
#             {
#                 "bin_id": bin_id,
#                 "timestamp": datetime.utcnow(),
#                 "sensor_type": "co2_ppm",
#                 "value": 400 + int(fill_level * random.uniform(10, 20))
#             }
#         ]

#         if (datetime.now() - self.last_capture[bin_id]) > self.camera_interval:
#             readings.append(self.sensors[bin_id]["camera"].capture(fill_level).__dict__)
#             self.last_capture[bin_id] = datetime.now()

#         return readings

#     def _generate_special_events(self):
#         events = []

#         # Citizen report
#         if random.random() < 0.1:
#             bin = random.choice(self.bins)
#             report = generate_citizen_report(bin['bin_id'])
#             report["timestamp"] = datetime.utcnow()
#             events.append({
#                 "topic": self.topics["reports"],
#                 "data": report
#             })

#         # Maintenance event
#         if random.random() < 0.05:
#             bin = random.choice(self.bins)
#             events.append({
#                 "topic": self.topics["maintenance"],
#                 "data": {
#                     "event_id": f"mnt-{datetime.now().strftime('%Y%m%d%H%M%S%f')}",
#                     "bin_id": bin['bin_id'],
#                     "action": random.choice(["emptied", "repaired", "serviced"]),
#                     "technician": f"tech-{random.randint(100, 999)}",
#                     "timestamp": datetime.utcnow()
#                 }
#             })
#         return events

#     def _send_bin_metadata(self):
#         for bin in self.bins:
#             # Convert date fields to datetime if they are date objects
#             if isinstance(bin.get("installation_date"), date):
#                 bin["installation_date"] = bin["installation_date"].isoformat()
#             if isinstance(bin.get("last_maintenance"), date):
#                 bin["last_maintenance"] = bin["last_maintenance"].isoformat()
#             self.producer.send(self.topics["bins"], value=bin)

#     def run(self, interval=10):
#         print(f"Starting waste bin simulator with {len(self.bins)} bins...")
#         try:
#             while True:
#                 self._send_bin_metadata()

#                 for bin in self.bins:
#                     sensor_data = self._generate_sensor_readings(bin['bin_id'])
#                     for reading in sensor_data:
#                         self.producer.send(self.topics["sensor"], value=reading)

#                 for event in self._generate_special_events():
#                     self.producer.send(event["topic"], value=event["data"])

#                 if datetime.now().second < 5:
#                     print(f"{datetime.now().isoformat()} - Sent data for {len(self.bins)} bins")
#                 time.sleep(interval)

#         except KeyboardInterrupt:
#             print("Simulation stopped by user")
#         finally:
#             self.producer.flush()
#             self.producer.close()


# if __name__ == "__main__":
#     import argparse

#     parser = argparse.ArgumentParser(description='Smart Waste Management IoT Simulator')
#     parser.add_argument('--kafka-brokers', default='localhost:9092')
#     parser.add_argument('--interval', type=int, default=10)
#     args = parser.parse_args()

#     simulator = WasteBinSimulator(kafka_brokers=args.kafka_brokers)
#     simulator.run(interval=args.interval)



# new simulator
import time
import json
import random
import threading
import asyncio
from datetime import datetime, timedelta, date
from kafka import KafkaProducer
from sensors import UltrasonicSensor, TemperatureSensor, GPSSensor
from data_generators import generate_bin_data, generate_citizen_report
import numpy as np
from typing import Dict, List, Optional
import logging
from dataclasses import dataclass
from enum import Enum
import hashlib
import uuid

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class BinStatus(Enum):
    NORMAL = "normal"
    FULL = "full"
    OVERFLOW = "overflow"
    MAINTENANCE_REQUIRED = "maintenance_required"
    OFFLINE = "offline"
    FIRE_DETECTED = "fire_detected"
    VANDALIZED = "vandalized"

class WasteType(Enum):
    GENERAL = "general"
    RECYCLABLE = "recyclable"
    ORGANIC = "organic"
    HAZARDOUS = "hazardous"

@dataclass
class SensorReading:
    bin_id: str
    sensor_type: str
    value: float
    unit: str
    timestamp: datetime
    quality_score: float
    anomaly_detected: bool = False

class AdvancedWasteBinSimulator:
    """
    AdvancedWasteBinSimulator simulates a smart waste management system with enhanced features such as anomaly detection,
    predictive analytics, security monitoring, and environmental impact calculations. It generates realistic sensor data,
    manages bin states, and communicates with Kafka for real-time data streaming.
    Attributes:
        bins (list): List of bin metadata dictionaries.
        sensors (dict): Dictionary mapping bin IDs to sensor objects and calibration data.
        bin_states (dict): Dictionary mapping bin IDs to their current state and history.
        weather_conditions (dict): Current weather conditions affecting waste management.
        anomaly_detector (AnomalyDetector): Detects anomalies in sensor readings.
        predictive_model (PredictiveMaintenanceModel): Predicts bin fill times and maintenance needs.
        security_monitor (SecurityMonitor): Handles data encryption and security events.
        route_optimizer (RouteOptimizer): Optimizes waste collection routes.
        simulation_speed (float): Speed multiplier for simulation cycles.
        sensor_failure_rate (float): Probability of sensor failure per cycle.
        network_latency_sim (bool): Enables network latency simulation.
        data_compression_enabled (bool): Enables Kafka data compression.
        encryption_enabled (bool): Enables encryption for sensitive data.
        topics (dict): Kafka topic names for different data streams.
        producer (KafkaProducer): Kafka producer for sending data.
    Methods:
        __init__(self, kafka_brokers='localhost:9092'):
            Initializes the simulator, sensors, bin states, weather, and Kafka producer.
        _initialize_sensors(self):
            # Initializes sensors for each bin, including calibration drift and failure probability.
        _initialize_bin_states(self):
            # Initializes the state and history for each bin, including fill patterns and waste type.
        _initialize_weather(self):
            # Sets initial weather conditions for the simulation.
        _generate_fill_pattern(self):
            # Generates realistic daily fill patterns for waste bins.
        _simulate_sensor_drift(self, sensor_reading, bin_id, sensor_type):
            # Simulates calibration drift and noise in sensor readings.
        _check_sensor_failure(self, bin_id):
            # Simulates random sensor failures based on probability.
        _generate_advanced_sensor_readings(self, bin_id):
            # Generates advanced sensor readings for a bin, including anomaly detection and fire/vandalism alerts.
        _calculate_decomposition_heat(self, fill_level, waste_type):
            # Calculates heat generated by waste decomposition based on fill level and type.
        _calculate_quality_score(self, value, sensor_type):
            # Calculates data quality score for sensor readings.
        _calculate_co2_levels(self, fill_level, temperature):
            # Estimates CO2 levels based on fill level and temperature.
        _calculate_humidity(self, fill_level):
            # Estimates humidity inside the bin based on fill level and weather.
        _calculate_methane_levels(self, fill_level, waste_type):
            # Estimates methane emissions based on fill level and waste type.
        _detect_vibration_patterns(self, bin_id):
            # Detects unusual vibration patterns indicating vandalism.
        _monitor_sound_levels(self, bin_id):
            # Monitors ambient sound levels near the bin.
        _simulate_gps_drift(self, original_coords, bin_id):
            # Simulates GPS drift and detects significant bin movement (potential theft).
        _generate_alert(self, bin_id, alert_type, description):
            # Generates and sends system alerts to Kafka, with optional encryption.
        _determine_severity(self, alert_type):
            # Determines severity level for different alert types.
        _generate_predictive_analytics(self):
            # Generates and sends predictive analytics data to Kafka.
        _predict_collections(self):
            # Predicts the number of bins requiring collection today.
        _calculate_environmental_metrics(self):
            # Calculates environmental impact metrics such as methane emissions and recycling rate.
        _calculate_recycling_rate(self):
            # Calculates the estimated recycling rate across all bins.
        _calculate_system_health(self):
            # Calculates overall system health metrics, including uptime and data quality.
        _update_weather_conditions(self):
            # Updates weather conditions affecting waste management.
        run(self, interval=10):
            # Runs the main simulation loop, generating sensor data, events, and analytics.
        _run_analytics_loop(self):
            # Runs analytics generation in a separate thread.
        _send_bin_metadata(self):
            # Sends enriched bin metadata to Kafka.
        _generate_special_events(self):
            # Generates special events such as citizen reports and maintenance actions.
    """
    def __init__(self, kafka_brokers='localhost:9092'):
        self.bins = generate_bin_data(100)
        self.sensors = self._initialize_sensors()
        self.bin_states = self._initialize_bin_states()
        self.weather_conditions = self._initialize_weather()
        self.anomaly_detector = AnomalyDetector()
        self.predictive_model = PredictiveMaintenanceModel()
        self.security_monitor = SecurityMonitor()
        self.route_optimizer = RouteOptimizer()
        
        self.simulation_speed = 1.0
        self.sensor_failure_rate = 0.001
        self.network_latency_sim = True
        self.data_compression_enabled = True
        self.encryption_enabled = True
        
        self.topics = {
            "bins": "bin-metadata",
            "sensor": "waste-sensor-data",
            "reports": "citizen-reports",
            "maintenance": "maintenance-events",
            "alerts": "system-alerts",
            "analytics": "analytics-data",
            "security": "security-events",
            "routes": "optimized-routes"
        }

        logger.info(f"Connecting to Kafka at {kafka_brokers}...")
        
        def json_serializer(obj):
            if isinstance(obj, (datetime, date)):
                return obj.isoformat()
            elif isinstance(obj, Enum):
                return obj.value
            elif isinstance(obj, np.ndarray):
                return obj.tolist()
            return str(obj)

        self.producer = KafkaProducer(
            bootstrap_servers=kafka_brokers,
            value_serializer=lambda v: json.dumps(v, default=json_serializer).encode('utf-8'),
            retries=5,
            batch_size=16384,
            linger_ms=10,
            compression_type='gzip' if self.data_compression_enabled else None
        )
        logger.info("Connected to Kafka!")

    def _initialize_sensors(self):
        sensors = {}
        for bin_data in self.bins:
            bin_id = bin_data['bin_id']
            lat = bin_data['location']['coordinates']['lat']
            lon = bin_data['location']['coordinates']['lon']
            capacity = bin_data['capacity_kg']
            
            sensors[bin_id] = {
                "ultrasonic": UltrasonicSensor(bin_id, capacity),
                "temperature": TemperatureSensor(bin_id),
                "gps": GPSSensor(bin_id, lat, lon),
                "last_reading": datetime.now(),
                "failure_probability": random.uniform(0.001, 0.01),
                "calibration_drift": random.uniform(-0.05, 0.05)
            }
        return sensors

    def _initialize_bin_states(self):
        states = {}
        for bin_data in self.bins:
            bin_id = bin_data['bin_id']
            states[bin_id] = {
                "status": BinStatus.NORMAL,
                "waste_type": random.choice(list(WasteType)),
                "fill_pattern": self._generate_fill_pattern(),
                "last_emptied": datetime.now() - timedelta(days=random.randint(0, 7)),
                "collection_frequency": random.randint(1, 7),  # days
                "temperature_history": [],
                "fill_history": [],
                "predicted_full_time": None,
                "energy_consumption": 0.0,
                "network_quality": random.uniform(0.7, 1.0)
            }
        return states

    def _initialize_weather(self):
        return {
            "temperature": 20.0,
            "humidity": 50.0,
            "wind_speed": 5.0,
            "precipitation": 0.0,
            "uv_index": 3.0,
            "timestamp": datetime.now()
        }

    def _generate_fill_pattern(self):
        """Generate realistic daily fill patterns"""
        hours = np.arange(24)
        # Morning rush, lunch peak, evening peak
        pattern = (0.3 * np.exp(-((hours - 8) ** 2) / 8) + 
                  0.5 * np.exp(-((hours - 12) ** 2) / 6) +
                  0.7 * np.exp(-((hours - 18) ** 2) / 10) +
                  0.1 * np.random.normal(0, 0.1, 24))
        return np.maximum(0, pattern)

    def _simulate_sensor_drift(self, sensor_reading: float, bin_id: str, sensor_type: str) -> float:
        """Simulate sensor calibration drift over time"""
        drift = self.sensors[bin_id]["calibration_drift"]
        noise = random.gauss(0, 0.02)  # 2% noise
        return max(0, sensor_reading + drift + noise)

    def _check_sensor_failure(self, bin_id: str) -> bool:
        """Simulate sensor failures"""
        failure_prob = self.sensors[bin_id]["failure_probability"]
        return random.random() < failure_prob

    def _generate_advanced_sensor_readings(self, bin_id: str) -> List[SensorReading]:
        if self._check_sensor_failure(bin_id):
            logger.warning(f"Sensor failure detected for bin {bin_id}")
            return []

        bin_state = self.bin_states[bin_id]
        current_hour = datetime.now().hour
        
        # Get realistic fill level based on time patterns
        base_fill = bin_state["fill_pattern"][current_hour] * 100
        days_since_empty = (datetime.now() - bin_state["last_emptied"]).days
        accumulated_fill = min(95, base_fill + (days_since_empty * 15))
        
        fill_level = self._simulate_sensor_drift(accumulated_fill, bin_id, "ultrasonic")
        
        # Update status based on fill level
        if fill_level > 95:
            bin_state["status"] = BinStatus.OVERFLOW
        elif fill_level > 85:
            bin_state["status"] = BinStatus.FULL
        
        capacity = next(b["capacity_kg"] for b in self.bins if b["bin_id"] == bin_id)
        weight = round((fill_level / 100) * capacity, 1)
        
        # Advanced temperature simulation with decomposition modeling
        base_temp = self.weather_conditions["temperature"]
        decomp_heat = self._calculate_decomposition_heat(fill_level, bin_state["waste_type"])
        temperature = self._simulate_sensor_drift(
            base_temp + decomp_heat + random.uniform(-2, 2), bin_id, "temperature"
        )
        
        # Fire detection
        if temperature > 60:
            bin_state["status"] = BinStatus.FIRE_DETECTED
            self._generate_alert(bin_id, "FIRE_DETECTED", f"Temperature: {temperature:.1f}°C")
        
        readings = [
            SensorReading(bin_id, "fill_level", fill_level, "%", datetime.utcnow(), 
                         self._calculate_quality_score(fill_level, "ultrasonic")),
            SensorReading(bin_id, "temperature", temperature, "°C", datetime.utcnow(),
                         self._calculate_quality_score(temperature, "temperature")),
            SensorReading(bin_id, "weight", weight, "kg", datetime.utcnow(), 0.95),
            SensorReading(bin_id, "co2", self._calculate_co2_levels(fill_level, temperature), "ppm", 
                         datetime.utcnow(), 0.92),
            SensorReading(bin_id, "humidity", self._calculate_humidity(fill_level), "%", 
                         datetime.utcnow(), 0.88),
            SensorReading(bin_id, "methane", self._calculate_methane_levels(fill_level, bin_state["waste_type"]), 
                         "ppm", datetime.utcnow(), 0.90),
            SensorReading(bin_id, "vibration", self._detect_vibration_patterns(bin_id), "Hz", 
                         datetime.utcnow(), 0.94),
            SensorReading(bin_id, "sound_level", self._monitor_sound_levels(bin_id), "dB", 
                         datetime.utcnow(), 0.87)
        ]
        
        # Add GPS with location drift simulation
        gps_reading = self.sensors[bin_id]["gps"].measure()
        gps_reading.value = self._simulate_gps_drift(gps_reading.value, bin_id)
        readings.append(SensorReading(bin_id, "gps", gps_reading.value, "coordinates", 
                                    datetime.utcnow(), 0.96))
        
        # Update historical data
        bin_state["fill_history"].append({"timestamp": datetime.now(), "fill_level": fill_level})
        bin_state["temperature_history"].append({"timestamp": datetime.now(), "temperature": temperature})
        
        # Keep only last 100 readings for performance
        if len(bin_state["fill_history"]) > 100:
            bin_state["fill_history"] = bin_state["fill_history"][-100:]
            bin_state["temperature_history"] = bin_state["temperature_history"][-100:]
        
        # Anomaly detection
        for reading in readings:
            reading.anomaly_detected = self.anomaly_detector.detect_anomaly(reading)
        
        return readings

    def _calculate_decomposition_heat(self, fill_level: float, waste_type: WasteType) -> float:
        """Calculate heat generated by decomposition"""
        base_heat = {
            WasteType.ORGANIC: 8.0,
            WasteType.GENERAL: 3.0,
            WasteType.RECYCLABLE: 1.0,
            WasteType.HAZARDOUS: 5.0
        }
        return (fill_level / 100) * base_heat.get(waste_type, 2.0)

    def _calculate_quality_score(self, value: float, sensor_type: str) -> float:
        """Calculate data quality score based on sensor characteristics"""
        base_score = 0.95
        if sensor_type == "ultrasonic" and (value < 0 or value > 100):
            base_score -= 0.3
        elif sensor_type == "temperature" and (value < -40 or value > 80):
            base_score -= 0.2
        return max(0.1, base_score + random.uniform(-0.1, 0.05))

    def _calculate_co2_levels(self, fill_level: float, temperature: float) -> float:
        base_co2 = 400
        decomp_co2 = fill_level * temperature * 0.1
        return base_co2 + decomp_co2 + random.uniform(-20, 20)

    def _calculate_humidity(self, fill_level: float) -> float:
        base_humidity = self.weather_conditions["humidity"]
        bin_humidity = fill_level * 0.3
        return min(100, base_humidity + bin_humidity + random.uniform(-5, 5))

    def _calculate_methane_levels(self, fill_level: float, waste_type: WasteType) -> float:
        base_methane = {
            WasteType.ORGANIC: 50,
            WasteType.GENERAL: 20,
            WasteType.RECYCLABLE: 5,
            WasteType.HAZARDOUS: 30
        }
        return base_methane.get(waste_type, 10) * (fill_level / 100) + random.uniform(0, 10)

    def _detect_vibration_patterns(self, bin_id: str) -> float:
        """Detect vandalism or unusual activity through vibration"""
        base_vibration = random.uniform(0, 2)
        if random.random() < 0.001:  # Rare vandalism event
            self.bin_states[bin_id]["status"] = BinStatus.VANDALIZED
            self._generate_alert(bin_id, "VANDALISM_DETECTED", "Unusual vibration patterns detected")
            return random.uniform(10, 50)
        return base_vibration

    def _monitor_sound_levels(self, bin_id: str) -> float:
        """Monitor ambient sound levels"""
        hour = datetime.now().hour
        if 6 <= hour <= 22:  # Day time
            return random.uniform(40, 70)
        else:  # Night time
            return random.uniform(25, 45)

    def _simulate_gps_drift(self, original_coords: dict, bin_id: str) -> dict:
        """Simulate GPS drift and detect bin movement"""
        drift_lat = random.uniform(-0.0001, 0.0001)
        drift_lon = random.uniform(-0.0001, 0.0001)
        
        new_coords = {
            "lat": original_coords["lat"] + drift_lat,
            "lon": original_coords["lon"] + drift_lon
        }
        
        # Detect significant movement (potential theft)
        distance = abs(drift_lat) + abs(drift_lon)
        if distance > 0.001:  # Significant movement
            self._generate_alert(bin_id, "BIN_MOVED", f"Bin moved {distance:.4f} degrees")
        
        return new_coords

    def _generate_alert(self, bin_id: str, alert_type: str, description: str):
        """Generate system alerts"""
        alert = {
            "alert_id": str(uuid.uuid4()),
            "bin_id": bin_id,
            "alert_type": alert_type,
            "description": description,
            "severity": self._determine_severity(alert_type),
            "timestamp": datetime.utcnow(),
            "resolved": False
        }
        
        if self.encryption_enabled:
            alert = self.security_monitor.encrypt_data(alert)
        
        self.producer.send(self.topics["alerts"], value=alert)
        logger.warning(f"Alert generated: {alert_type} for bin {bin_id}")

    def _determine_severity(self, alert_type: str) -> str:
        severity_map = {
            "FIRE_DETECTED": "CRITICAL",
            "VANDALISM_DETECTED": "HIGH",
            "BIN_MOVED": "MEDIUM",
            "OVERFLOW": "HIGH",
            "SENSOR_FAILURE": "MEDIUM"
        }
        return severity_map.get(alert_type, "LOW")

    def _generate_predictive_analytics(self):
        """Generate predictive analytics data"""
        analytics = {
            "timestamp": datetime.utcnow(),
            "total_bins": len(self.bins),
            "bins_requiring_attention": sum(1 for state in self.bin_states.values() 
                                          if state["status"] != BinStatus.NORMAL),
            "average_fill_level": np.mean([
                state["fill_history"][-1]["fill_level"] if state["fill_history"] else 0
                for state in self.bin_states.values()
            ]),
            "predicted_collections_today": self._predict_collections(),
            "optimal_routes": self.route_optimizer.calculate_optimal_routes(self.bins, self.bin_states),
            "environmental_impact": self._calculate_environmental_metrics(),
            "system_health": self._calculate_system_health()
        }
        
        self.producer.send(self.topics["analytics"], value=analytics)

    def _predict_collections(self) -> int:
        """Predict number of collections needed today"""
        full_bins = sum(1 for state in self.bin_states.values() 
                       if state["status"] in [BinStatus.FULL, BinStatus.OVERFLOW])
        predicted_full = self.predictive_model.predict_full_bins(self.bin_states)
        return full_bins + predicted_full

    def _calculate_environmental_metrics(self) -> dict:
        """Calculate environmental impact metrics"""
        total_methane = sum(self._calculate_methane_levels(
            state["fill_history"][-1]["fill_level"] if state["fill_history"] else 0,
            state["waste_type"]
        ) for state in self.bin_states.values())
        
        return {
            "total_methane_emission": total_methane,
            "co2_equivalent": total_methane * 25,  # Methane is 25x more potent than CO2
            "estimated_waste_volume": sum(
                state["fill_history"][-1]["fill_level"] if state["fill_history"] else 0
                for state in self.bin_states.values()
            ),
            "recycling_rate": self._calculate_recycling_rate()
        }

    def _calculate_recycling_rate(self) -> float:
        """Calculate estimated recycling rate"""
        recyclable_bins = sum(1 for state in self.bin_states.values() 
                            if state["waste_type"] == WasteType.RECYCLABLE)
        return (recyclable_bins / len(self.bins)) * 100

    def _calculate_system_health(self) -> dict:
        """Calculate overall system health metrics"""
        offline_bins = sum(1 for state in self.bin_states.values() 
                          if state["status"] == BinStatus.OFFLINE)
        
        return {
            "system_uptime": ((len(self.bins) - offline_bins) / len(self.bins)) * 100,
            "average_network_quality": np.mean([
                state["network_quality"] for state in self.bin_states.values()
            ]),
            "data_quality_score": np.mean([
                0.9 if state["status"] == BinStatus.NORMAL else 0.7
                for state in self.bin_states.values()
            ]) * 100,
            "maintenance_alerts": sum(1 for state in self.bin_states.values() 
                                   if state["status"] == BinStatus.MAINTENANCE_REQUIRED)
        }

    def _update_weather_conditions(self):
        """Update weather conditions affecting waste management"""
        self.weather_conditions.update({
            "temperature": self.weather_conditions["temperature"] + random.uniform(-2, 2),
            "humidity": max(0, min(100, self.weather_conditions["humidity"] + random.uniform(-5, 5))),
            "wind_speed": max(0, self.weather_conditions["wind_speed"] + random.uniform(-2, 2)),
            "precipitation": max(0, random.uniform(0, 2) if random.random() < 0.1 else 0),
            "timestamp": datetime.now()
        })

    def run(self, interval=10):
        """Run the advanced simulation"""
        logger.info(f"Starting advanced waste bin simulator with {len(self.bins)} bins...")
        logger.info("Advanced features: Anomaly Detection, Predictive Analytics, Security Monitoring")
        
        analytics_thread = threading.Thread(target=self._run_analytics_loop)
        analytics_thread.daemon = True
        analytics_thread.start()
        
        try:
            cycle_count = 0
            while True:
                start_time = time.time()
                
                # Update weather every 10 cycles
                if cycle_count % 10 == 0:
                    self._update_weather_conditions()
                
                # Send bin metadata less frequently
                if cycle_count % 60 == 0:
                    self._send_bin_metadata()
                
                # Generate sensor readings for all bins
                for bin_data in self.bins:
                    readings = self._generate_advanced_sensor_readings(bin_data['bin_id'])
                    for reading in readings:
                        reading_dict = {
                            "bin_id": reading.bin_id,
                            "sensor_type": reading.sensor_type,
                            "value": reading.value,
                            "unit": reading.unit,
                            "timestamp": reading.timestamp,
                            "quality_score": reading.quality_score,
                            "anomaly_detected": reading.anomaly_detected
                        }
                        
                        if self.encryption_enabled:
                            reading_dict = self.security_monitor.encrypt_data(reading_dict)
                        
                        self.producer.send(self.topics["sensor"], value=reading_dict)
                
                # Generate special events
                for event in self._generate_special_events():
                    self.producer.send(event["topic"], value=event["data"])
                
                # Simulate network latency
                if self.network_latency_sim:
                    time.sleep(random.uniform(0.01, 0.1))
                
                processing_time = time.time() - start_time
                sleep_time = max(0, (interval / self.simulation_speed) - processing_time)
                
                if cycle_count % 60 == 0:
                    logger.info(f"Cycle {cycle_count}: Processed {len(self.bins)} bins in {processing_time:.2f}s")
                
                time.sleep(sleep_time)
                cycle_count += 1
                
        except KeyboardInterrupt:
            logger.info("Simulation stopped by user")
        finally:
            self.producer.flush()
            self.producer.close()

    def _run_analytics_loop(self):
        """Run analytics in separate thread"""
        while True:
            try:
                self._generate_predictive_analytics()
                time.sleep(60)  # Generate analytics every minute
            except Exception as e:
                logger.error(f"Analytics error: {e}")
                time.sleep(60)

    def _send_bin_metadata(self):
        """Send enriched bin metadata"""
        for bin_data in self.bins:
            bin_id = bin_data['bin_id']
            enriched_bin = bin_data.copy()
            enriched_bin.update({
                "current_status": self.bin_states[bin_id]["status"].value,
                "waste_type": self.bin_states[bin_id]["waste_type"].value,
                "last_emptied": self.bin_states[bin_id]["last_emptied"],
                "collection_frequency": self.bin_states[bin_id]["collection_frequency"],
                "predicted_full_time": self.predictive_model.predict_full_time(bin_id, self.bin_states[bin_id]),
                "network_quality": self.bin_states[bin_id]["network_quality"]
            })
            
            if isinstance(enriched_bin.get("installation_date"), date):
                enriched_bin["installation_date"] = enriched_bin["installation_date"].isoformat()
            if isinstance(enriched_bin.get("last_maintenance"), date):
                enriched_bin["last_maintenance"] = enriched_bin["last_maintenance"].isoformat()
            
            self.producer.send(self.topics["bins"], value=enriched_bin)

    def _generate_special_events(self):
        """Generate enhanced special events"""
        events = []
        
        # Citizen report with sentiment analysis
        if random.random() < 0.08:
            bin_data = random.choice(self.bins)
            report = generate_citizen_report(bin_data['bin_id'])
            report.update({
                "timestamp": datetime.utcnow(),
                "sentiment": random.choice(["positive", "neutral", "negative"]),
                "priority": random.choice(["low", "medium", "high"]),
                "location_verified": random.choice([True, False])
            })
            events.append({"topic": self.topics["reports"], "data": report})
        
        # Advanced maintenance events
        if random.random() < 0.03:
            bin_data = random.choice(self.bins)
            maintenance_event = {
                "event_id": f"mnt-{datetime.now().strftime('%Y%m%d%H%M%S%f')}",
                "bin_id": bin_data['bin_id'],
                "action": random.choice(["emptied", "repaired", "serviced", "cleaned", "sensor_calibrated"]),
                "technician": f"tech-{random.randint(100, 999)}",
                "timestamp": datetime.utcnow(),
                "duration_minutes": random.randint(5, 45),
                "parts_replaced": random.choice([None, "sensor", "battery", "communication_module"]),
                "cost": round(random.uniform(20, 200), 2)
            }
            
            # Reset bin status after maintenance
            if maintenance_event["action"] == "emptied":
                self.bin_states[bin_data['bin_id']]["last_emptied"] = datetime.now()
                self.bin_states[bin_data['bin_id']]["status"] = BinStatus.NORMAL
            
            events.append({"topic": self.topics["maintenance"], "data": maintenance_event})
        
        return events


class AnomalyDetector:
    """Advanced anomaly detection using statistical methods"""
    
    def __init__(self):
        self.thresholds = {
            "fill_level": {"min": 0, "max": 100, "std_multiplier": 3},
            "temperature": {"min": -10, "max": 70, "std_multiplier": 2.5},
            "co2": {"min": 300, "max": 2000, "std_multiplier": 2},
            "methane": {"min": 0, "max": 500, "std_multiplier": 2}
        }
    
    def detect_anomaly(self, reading: SensorReading) -> bool:
        """Detect anomalies in sensor readings"""
        threshold = self.thresholds.get(reading.sensor_type)
        if not threshold:
            return False
        
        # Range-based detection
        if reading.value < threshold["min"] or reading.value > threshold["max"]:
            return True
        
        # Quality-based detection
        if reading.quality_score < 0.5:
            return True
        
        return False


class PredictiveMaintenanceModel:
    """Predictive maintenance using simple ML concepts"""
    
    def predict_full_bins(self, bin_states: dict) -> int:
        """Predict how many bins will be full in next 24 hours"""
        predictions = 0
        for bin_id, state in bin_states.items():
            if state["fill_history"]:
                recent_fills = [entry["fill_level"] for entry in state["fill_history"][-10:]]
                if len(recent_fills) >= 3:
                    trend = (recent_fills[-1] - recent_fills[0]) / len(recent_fills)
                    if recent_fills[-1] + (trend * 24) > 85:  # Predict full in 24 hours
                        predictions += 1
        return predictions
    
    def predict_full_time(self, bin_id: str, bin_state: dict) -> Optional[datetime]:
        """Predict when a bin will be full"""
        if not bin_state["fill_history"] or len(bin_state["fill_history"]) < 5:
            return None
        
        recent_fills = [entry["fill_level"] for entry in bin_state["fill_history"][-10:]]
        times = [entry["timestamp"] for entry in bin_state["fill_history"][-10:]]
        
        if len(recent_fills) >= 3:
            # Simple linear regression
            current_level = recent_fills[-1]
            time_diff = (times[-1] - times[0]).total_seconds() / 3600  # hours
            fill_rate = (recent_fills[-1] - recent_fills[0]) / time_diff if time_diff > 0 else 0
            
            if fill_rate > 0:
                hours_to_full = (85 - current_level) / fill_rate
                return datetime.now() + timedelta(hours=hours_to_full)
        
        return None


class SecurityMonitor:
    """Security and encryption features"""
    
    def encrypt_data(self, data: dict) -> dict:
        """Simple data hashing for security simulation"""
        data_copy = data.copy()
        data_copy["security_hash"] = hashlib.md5(
            json.dumps(data, sort_keys=True).encode()
        ).hexdigest()[:16]
        return data_copy


class RouteOptimizer:
    """Route optimization for waste collection"""
    
    def calculate_optimal_routes(self, bins: list, bin_states: dict) -> list:
        """Calculate optimal collection routes"""
        full_bins = [
            bin_data for bin_data in bins 
            if bin_states[bin_data['bin_id']]["status"] in [BinStatus.FULL, BinStatus.OVERFLOW]
        ]
        
        if not full_bins:
            return []
        
        routes = []
        route_size = 10  
        
        for i in range(0, len(full_bins), route_size):
            route_bins = full_bins[i:i + route_size]
            route = {
                "route_id": f"route-{len(routes) + 1}",
                "bins": [bin_data['bin_id'] for bin_data in route_bins],
                "estimated_time": len(route_bins) * 8,  # 8 minutes per bin
                "estimated_distance": len(route_bins) * 0.5,  # 0.5 km per bin
                "priority": "high" if any(
                    bin_states[bin_data['bin_id']]["status"] == BinStatus.OVERFLOW 
                    for bin_data in route_bins
                ) else "normal"
            }
            routes.append(route)
        
        return routes


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Advanced Smart Waste Management IoT Simulator')
    parser.add_argument('--kafka-brokers', default='localhost:9092')
    parser.add_argument('--interval', type=int, default=10, help='Sensor reading interval in seconds')
    parser.add_argument('--speed', type=float, default=1.0, help='')