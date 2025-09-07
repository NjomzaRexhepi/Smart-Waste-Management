import time
import json
import random
import uuid
from datetime import datetime, date, timezone
from kafka import KafkaProducer
from sensors import UltrasonicSensor, TemperatureSensor, GPSSensor
from data_generators import generate_bin_data

class WasteBinSimulator:
    def __init__(self, kafka_brokers='localhost:9092', num_bins=50):
        raw_bins = generate_bin_data(num_bins)
        self.bins = []

        for b in raw_bins:
            bin_uuid = str(uuid.uuid4())
            lat = b.get('latitude') or (b.get('location') or {}).get('coordinates', {}).get('lat')
            lng = b.get('longitude') or (b.get('location') or {}).get('coordinates', {}).get('lon')
            capacity = b.get('capacity') or b.get('capacity_kg') or random.uniform(20.0, 240.0)
            current_fill = b.get('current_fill_level') or round(random.uniform(5.0, 75.0), 2)

            self.bins.append({
                "bin_id": bin_uuid,
                "location_lat": float(lat) if lat else round(random.uniform(-90, 90), 6),
                "location_lng": float(lng) if lng else round(random.uniform(-180, 180), 6),
                "type": b.get('type', 'standard'),
                "capacity": float(capacity),
                "current_fill_level": float(current_fill),
                "status": b.get('status', 'active'),
                "last_maintenance_date": b.get('last_maintenance'),
                "installation_date": b.get('installation_date'),
            })

        self.sensors = self._initialize_sensors()

        self.topics = {
            "bins": "bins",
            "sensor": "sensor_data",
            "reports": "citizen_reports",
            "maintenance": "maintenance_events",
            "alarms": "alarms",
            "performance": "performance_analytics",
            "routes": "route_history"
        }

        print(f"Connecting to Kafka at {kafka_brokers}...")

        def json_serializer(obj):
            if isinstance(obj, (datetime, date)):
                return obj.isoformat()
            return str(obj)

        self.producer = KafkaProducer(
            bootstrap_servers=kafka_brokers,
            value_serializer=lambda v: json.dumps(v, default=json_serializer).encode('utf-8'),
            retries=5
        )
        print("Connected to Kafka!")

    def _initialize_sensors(self):
        sensors = {}
        for b in self.bins:
            bin_id = b['bin_id']
            sensors[bin_id] = {
                "ultrasonic": UltrasonicSensor(bin_id, b["capacity"]),
                "temperature": TemperatureSensor(bin_id),
                "gps": GPSSensor(bin_id, b["location_lat"], b["location_lng"])
            }
        return sensors

    def _generate_sensor_row(self, bin_rec):
        bin_id = bin_rec['bin_id']
        ultrasonic = self.sensors[bin_id]["ultrasonic"].measure()
        fill_level = float(ultrasonic.value)
        weight = round((fill_level / 100.0) * bin_rec["capacity"], 2)
        temperature = float(self.sensors[bin_id]["temperature"].measure(fill_level).value)
        humidity = round(random.uniform(25.0, 85.0), 2)
        status = "alert" if fill_level >= 90 or temperature >= 60 else "normal"

        return {
            "sensor_id": str(uuid.uuid4()),
            "bin_id": bin_id,
            "timestamp": datetime.now(timezone.utc),
            "fill_level": fill_level,
            "temperature": temperature,
            "weight": weight,
            "humidity": humidity,
            "status": status
        }

    def _generate_special_events(self):
        events = []

        if random.random() < 0.10:
            b = random.choice(self.bins)
            events.append({"topic": self.topics["reports"], "data": {
                "report_id": str(uuid.uuid4()),
                "bin_id": b['bin_id'],
                "report_timestamp": datetime.now(timezone.utc),
                "report_type": random.choice(["overflow", "damage", "odor"]),
                "description": f"Issue reported for bin {b['bin_id']}",
                "resolved": random.choice([True, False]),
                "resolved_timestamp": None
            }})

        if random.random() < 0.05:
            b = random.choice(self.bins)
            events.append({"topic": self.topics["maintenance"], "data": {
                "event_id": str(uuid.uuid4()),
                "bin_id": b['bin_id'],  # ✅ UUID now
                "maintenance_type": random.choice(["emptied", "repaired"]),
                "event_timestamp": datetime.now(timezone.utc),
                "performed_by": f"tech-{random.randint(100,999)}",
                "duration": round(random.uniform(10, 120), 2),
                "cost": round(random.uniform(10.0, 50.0), 2)
            }})

        if random.random() < 0.05:
            b = random.choice(self.bins)
            events.append({"topic": self.topics["alarms"], "data": {
                "alarm_id": str(uuid.uuid4()),
                "bin_id": b['bin_id'],
                "alarm_type": random.choice(["overflow", "temperature", "co2_high"]),
                "triggered_at": datetime.now(timezone.utc),
                "resolved_at": None,
                "severity": random.choice(["low", "medium", "high"]),
                "status": "active"
            }})

        if random.random() < 0.02:
            events.append({"topic": self.topics["performance"], "data": {
                "id": str(uuid.uuid4()),
                "date": datetime.now(timezone.utc).date(),
                "total_bins": len(self.bins),
                "avg_fill_level": round(random.uniform(30, 90), 2),
                "total_reports": random.randint(0, 10),
                "resolved_reports": random.randint(0, 10),
                "maintenance_events_count": random.randint(0, 5),
                "avg_response_time": round(random.uniform(10, 120), 2),
                "alarms_count": random.randint(0, 5),
                "routes_completed": random.randint(1, 10)
            }})

        if random.random() < 0.03:
            route_id = str(uuid.uuid4())
            for b in random.sample(self.bins, random.randint(3, min(12, len(self.bins)))):
                events.append({"topic": self.topics["routes"], "data": {
                    "route_id": route_id,
                    "bin_id": b['bin_id'],
                    "service_date": datetime.now(timezone.utc),
                    "collection_status": random.choice(["collected", "delayed", "missed", "skipped"])
                }})

        return events

    def _send_bin_metadata(self):
        for b in self.bins:
            self.producer.send(self.topics["bins"], value=b)

    def run(self, interval=10):
        print(f"Starting simulator with {len(self.bins)} bins...")
        try:
            while True:
                self._send_bin_metadata()
                for b in self.bins:
                    self.producer.send(self.topics["sensor"], value=self._generate_sensor_row(b))
                for event in self._generate_special_events():
                    self.producer.send(event["topic"], value=event["data"])
                print(f"{datetime.now(timezone.utc).isoformat()} - Sent data for {len(self.bins)} bins")
                time.sleep(interval)
        except KeyboardInterrupt:
            print("Simulation stopped")
        finally:
            self.producer.flush()
            self.producer.close()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Waste Bin Simulator')
    parser.add_argument('--kafka-brokers', default='localhost:9092')
    parser.add_argument('--interval', type=int, default=10)
    parser.add_argument('--num-bins', type=int, default=50)
    args = parser.parse_args()

    sim = WasteBinSimulator(kafka_brokers=args.kafka_brokers, num_bins=args.num_bins)
    sim.run(interval=args.interval)
