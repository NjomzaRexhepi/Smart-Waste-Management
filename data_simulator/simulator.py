import time
import json
import random
from datetime import datetime
from kafka import KafkaProducer
from .sensors import UltrasonicSensor, TemperatureSensor, GPSSensor, CameraSensor
from .data_generators import generate_bin_data


class WasteBinSimulator:
    def __init__(self, kafka_brokers='localhost:9092', kafka_topic='waste-sensor-data'):
        self.bins = generate_bin_data(100)  # Generate 100 bins
        self.sensors = self._initialize_sensors()
        self.camera_interval = timedelta(minutes=15)
        self.last_capture = {}
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_brokers.split(','),
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.kafka_topic = kafka_topic

    def _initialize_sensors(self):
        """Create sensor instances for each bin"""
        sensors = {}
        for bin in self.bins:
            bin_id = bin['bin_id']
            lat = bin['location']['coordinates']['lat']
            lon = bin['location']['coordinates']['lon']

            sensors[bin_id] = {
                "ultrasonic": UltrasonicSensor(bin_id),
                "temperature": TemperatureSensor(bin_id),
                "gps": GPSSensor(bin_id, lat, lon),
                "camera": CameraSensor(bin_id)
            }
            self.last_capture[bin_id] = datetime.now()
        return sensors

    def _generate_sensor_readings(self, bin_id):
        """Collect data from all sensors for a bin"""
        readings = []

        # Get core sensors
        readings.append(self.sensors[bin_id]["ultrasonic"].measure())
        fill_level = readings[-1].value
        readings.append(self.sensors[bin_id]["temperature"].measure())
        readings.append(self.sensors[bin_id]["gps"].measure())

        # Capture image periodically
        if (datetime.now() - self.last_capture[bin_id]) > self.camera_interval:
            readings.append(self.sensors[bin_id]["camera"].capture(fill_level))
            self.last_capture[bin_id] = datetime.now()

        return [r.__dict__ for r in readings]

    def _generate_special_events(self):
        """Create random waste management events"""
        events = []
        # 10% chance of citizen report
        if random.random() < 0.1:
            bin = random.choice(self.bins)
            events.append({
                "event_type": "citizen_report",
                "data": generate_citizen_report(bin['bin_id']),
                "timestamp": datetime.utcnow().isoformat()
            })

        # 5% chance of maintenance event
        if random.random() < 0.05:
            bin = random.choice(self.bins)
            events.append({
                "event_type": "maintenance",
                "data": {
                    "bin_id": bin['bin_id'],
                    "action": random.choice(["emptied", "repaired", "serviced"]),
                    "technician": f"tech-{random.randint(100, 999)}"
                },
                "timestamp": datetime.utcnow().isoformat()
            })
        return events

    def run(self, interval=10):
        """Main simulation loop"""
        print(f"Starting waste bin simulator with {len(self.bins)} bins...")
        try:
            while True:
                # Simulate all bins
                for bin in self.bins:
                    bin_id = bin['bin_id']
                    sensor_data = self._generate_sensor_readings(bin_id)
                    for reading in sensor_data:
                        self.producer.send(self.kafka_topic, value=reading)

                # Generate special events
                for event in self._generate_special_events():
                    self.producer.send(self.kafka_topic, value=event)

                # Print status every minute
                if datetime.now().second < 5:  # Roughly once per minute
                    print(f"{datetime.now().isoformat()} - Sent data for {len(self.bins)} bins")

                time.sleep(interval)

        except KeyboardInterrupt:
            print("Simulation stopped by user")
        finally:
            self.producer.flush()
            self.producer.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Waste Management IoT Simulator')
    parser.add_argument('--kafka-brokers', default='localhost:9092',
                        help='Kafka bootstrap servers (comma separated)')
    parser.add_argument('--kafka-topic', default='waste-sensor-data',
                        help='Kafka topic for sensor data')
    parser.add_argument('--interval', type=int, default=10,
                        help='Simulation interval in seconds')
    args = parser.parse_args()

    simulator = WasteBinSimulator(
        kafka_brokers=args.kafka_brokers,
        kafka_topic=args.kafka_topic
    )
    simulator.run(interval=args.interval)