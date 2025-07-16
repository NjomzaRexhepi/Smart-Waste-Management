import time
import json
import random
from datetime import datetime, timedelta
from kafka import KafkaProducer
from sensors import UltrasonicSensor, TemperatureSensor, GPSSensor, CameraSensor
from data_generators import generate_bin_data, generate_citizen_report


class WasteBinSimulator:
    def __init__(self, kafka_brokers='localhost:9092', kafka_topic='waste-sensor-data'):
        self.bins = generate_bin_data(100)
        self.last_capture = {}
        self.sensors = self._initialize_sensors()
        self.camera_interval = timedelta(minutes=15)
        self.kafka_topic = kafka_topic

        print(f"Connecting to Kafka at {kafka_brokers}...")
        try:
            self.producer = KafkaProducer(
                bootstrap_servers='192.168.1.11:9092',
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                retries=5
            )
            print("Connected to Kafka!")
        except Exception as e:
            print(f"Failed to connect to Kafka: {e}")
            raise


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
        """Collect enriched data from all sensors for a bin"""
        gps_reading = self.sensors[bin_id]["gps"].measure()
        ultrasonic_reading = self.sensors[bin_id]["ultrasonic"].measure()
        temperature_reading = self.sensors[bin_id]["temperature"].measure()
        fill_level = ultrasonic_reading.value

        humidity = round(random.uniform(10, 90), 1)
        pressure = round(random.uniform(980, 1050), 1)
        sound_level = round(random.uniform(30, 100), 1)
        co2_ppm = round(random.uniform(400, 2000))
        battery = round(random.uniform(20, 100), 2)
        ping = round(random.uniform(10, 300), 1)
        motion = random.choice([True, False])
        status = random.choice(["OK", "ERROR", "OFFLINE"])

        timestamp = datetime.utcnow().isoformat()

        readings = [
            {
                "timestamp": timestamp,
                "bin_id": bin_id,
                "sensor_type": "ultrasonic",
                "measurement": fill_level
            },
            {
                "timestamp": timestamp,
                "bin_id": bin_id,
                "sensor_type": "temperature",
                "measurement": temperature_reading.value
            },
            {
                "timestamp": timestamp,
                "bin_id": bin_id,
                "sensor_type": "gps",
                "measurement": gps_reading.value
            },
            {
                "timestamp": timestamp,
                "bin_id": bin_id,
                "sensor_type": "humidity",
                "measurement": humidity
            },
            {
                "timestamp": timestamp,
                "bin_id": bin_id,
                "sensor_type": "pressure",
                "measurement": pressure
            },
            {
                "timestamp": timestamp,
                "bin_id": bin_id,
                "sensor_type": "sound_level_db",
                "measurement": sound_level
            },
            {
                "timestamp": timestamp,
                "bin_id": bin_id,
                "sensor_type": "co2_ppm",
                "measurement": co2_ppm
            },
            {
                "timestamp": timestamp,
                "bin_id": bin_id,
                "sensor_type": "battery",
                "measurement": battery
            },
            {
                "timestamp": timestamp,
                "bin_id": bin_id,
                "sensor_type": "ping_ms",
                "measurement": ping
            },
            {
                "timestamp": timestamp,
                "bin_id": bin_id,
                "sensor_type": "motion",
                "measurement": motion
            },
            {
                "timestamp": timestamp,
                "bin_id": bin_id,
                "sensor_type": "status",
                "measurement": status
            }
        ]

        if (datetime.now() - self.last_capture[bin_id]) > self.camera_interval:
            image_reading = self.sensors[bin_id]["camera"].capture(fill_level)
            readings.append(image_reading.__dict__)
            self.last_capture[bin_id] = datetime.now()

        return readings

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
                for bin in self.bins:
                    bin_id = bin['bin_id']
                    sensor_data = self._generate_sensor_readings(bin_id)
                    for reading in sensor_data:
                        self.producer.send(self.kafka_topic, value=reading)

                for event in self._generate_special_events():
                    self.producer.send(self.kafka_topic, value=event)

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