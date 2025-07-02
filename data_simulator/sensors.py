import random
from datetime import datetime
from dataclasses import dataclass


@dataclass
class SensorReading:
    bin_id: str
    timestamp: str
    sensor_type: str
    value: float
    unit: str = None


class UltrasonicSensor:
    """Simulates fill level detection using ultrasonic waves"""

    def __init__(self, bin_id):
        self.bin_id = bin_id
        self.fill_level = 0.0
        self.sensor_type = "ultrasonic"

    def measure(self):
        fill_increase = random.uniform(0.5, 3.0)

        if random.random() < 0.05:
            self.fill_level = 0.0
        else:
            self.fill_level = min(100.0, self.fill_level + fill_increase)

        return SensorReading(
            bin_id=self.bin_id,
            timestamp=datetime.utcnow().isoformat(),
            sensor_type=self.sensor_type,
            value=round(self.fill_level, 2),
            unit="%"
        )


class TemperatureSensor:
    """Simulates internal bin temperature with organic waste spikes"""

    def __init__(self, bin_id):
        self.bin_id = bin_id
        self.sensor_type = "temperature"
        self.base_temp = random.uniform(18.0, 25.0)

    def measure(self):
        if random.random() < 0.15:
            temp = self.base_temp + random.uniform(5.0, 20.0)
        else:
            temp = self.base_temp + random.uniform(-2.0, 2.0)

        return SensorReading(
            bin_id=self.bin_id,
            timestamp=datetime.utcnow().isoformat(),
            sensor_type=self.sensor_type,
            value=round(temp, 1),
            unit="°C"
        )


class GPSSensor:

    def __init__(self, bin_id, lat, lon):
        self.bin_id = bin_id
        self.sensor_type = "gps"
        self.base_lat = lat
        self.base_lon = lon

    def measure(self):
        return SensorReading(
            bin_id=self.bin_id,
            timestamp=datetime.utcnow().isoformat(),
            sensor_type=self.sensor_type,
            value={
                "lat": self.base_lat + random.uniform(-0.0001, 0.0001),
                "lon": self.base_lon + random.uniform(-0.0001, 0.0001)
            }
        )


class CameraSensor:

    def __init__(self, bin_id):
        self.bin_id = bin_id
        self.sensor_type = "camera"

    def capture(self, fill_level):
        if fill_level < 30:
            status = "empty"
        elif fill_level < 80:
            status = "partial"
        else:
            status = "full"

        return SensorReading(
            bin_id=self.bin_id,
            timestamp=datetime.utcnow().isoformat(),
            sensor_type=self.sensor_type,
            value={
                "status": status,
                "confidence": random.uniform(0.85, 0.98)
            }
        )