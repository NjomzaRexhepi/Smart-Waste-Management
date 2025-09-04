import time
import json
import random
from datetime import datetime, timedelta, date
from kafka import KafkaProducer
from sensors import UltrasonicSensor, TemperatureSensor, GPSSensor, CameraSensor
from data_generators import generate_bin_data, generate_citizen_report

class WasteBinSimulator:
    def __init__(self, kafka_brokers='localhost:9092'):
        # Generate 50 bins
        self.bins = generate_bin_data(50)
        self.last_capture = {}
        self.sensors = self._initialize_sensors()
        self.camera_interval = timedelta(minutes=15)

        # Topics
        self.topics = {
            "bins": "bin-metadata",
            "sensor": "waste-sensor-data",
            "reports": "citizen-reports",
            "maintenance": "maintenance-events"
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
            # If your generator provides nested location, adapt here:
            # lat = b['location']['coordinates']['lat']; lon = b['location']['coordinates']['lon']
            lat = b.get('latitude') or b['location']['coordinates']['lat']
            lon = b.get('longitude') or b['location']['coordinates']['lon']
            capacity = b['capacity_kg']

            sensors[bin_id] = {
                "ultrasonic": UltrasonicSensor(bin_id, capacity),
                "temperature": TemperatureSensor(bin_id),
                "gps": GPSSensor(bin_id, lat, lon),
                "camera": CameraSensor(bin_id)
            }
            self.last_capture[bin_id] = datetime.now()
        return sensors

    def _generate_sensor_readings(self, bin_id):
        ultrasonic = self.sensors[bin_id]["ultrasonic"].measure()
        fill_level = ultrasonic.value
        capacity = next(b["capacity_kg"] for b in self.bins if b["bin_id"] == bin_id)
        weight = round((fill_level / 100) * capacity, 1)

        readings = [
            ultrasonic.__dict__,
            self.sensors[bin_id]["temperature"].measure(fill_level).__dict__,
            self.sensors[bin_id]["gps"].measure().__dict__,
            {
                "bin_id": bin_id,
                "timestamp": datetime.utcnow(),
                "sensor_type": "weight",
                "value": weight,
                "unit": "kg"
            },
            {
                "bin_id": bin_id,
                "timestamp": datetime.utcnow(),
                "sensor_type": "co2_ppm",
                "value": 400 + int(fill_level * random.uniform(10, 20))
            }
        ]

        if (datetime.now() - self.last_capture[bin_id]) > self.camera_interval:
            readings.append(self.sensors[bin_id]["camera"].capture(fill_level).__dict__)
            self.last_capture[bin_id] = datetime.now()

        return readings

    def _generate_special_events(self):
        events = []

        if random.random() < 0.1:
            b = random.choice(self.bins)
            report = generate_citizen_report(b['bin_id'])
            report["timestamp"] = datetime.utcnow()
            events.append({"topic": self.topics["reports"], "data": report})

        if random.random() < 0.05:
            b = random.choice(self.bins)
            events.append({
                "topic": self.topics["maintenance"],
                "data": {
                    "event_id": f"mnt-{datetime.now().strftime('%Y%m%d%H%M%S%f')}",
                    "bin_id": b['bin_id'],
                    "action": random.choice(["emptied", "repaired", "serviced"]),
                    "technician": f"tech-{random.randint(100, 999)}",
                    "timestamp": datetime.utcnow()
                }
            })
        return events

    def _send_bin_metadata(self):
        for b in self.bins:
            if isinstance(b.get("installation_date"), date):
                b["installation_date"] = b["installation_date"].isoformat()
            if isinstance(b.get("last_maintenance"), date):
                b["last_maintenance"] = b["last_maintenance"].isoformat()
            self.producer.send(self.topics["bins"], value=b)

    def run(self, interval=10):
        print(f"Starting waste bin simulator with {len(self.bins)} bins...")
        try:
            while True:
                self._send_bin_metadata()

                for b in self.bins:
                    sensor_data = self._generate_sensor_readings(b['bin_id'])
                    for reading in sensor_data:
                        self.producer.send(self.topics["sensor"], value=reading)

                for event in self._generate_special_events():
                    self.producer.send(event["topic"], value=event["data"])

                if datetime.now().second < 5:
                    print(f"{datetime.now().isoformat()} - Sent data for {len(self.bins)} bins")
                time.sleep(interval)

        except KeyboardInterrupt:
            print("Simulation stopped by user")
        finally:
            self.producer.flush()
            self.producer.close()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Smart Waste Management IoT Simulator')
    parser.add_argument('--kafka-brokers', default='localhost:9092')
    parser.add_argument('--interval', type=int, default=10)
    args = parser.parse_args()

    simulator = WasteBinSimulator(kafka_brokers=args.kafka_brokers)
    simulator.run(interval=args.interval)
