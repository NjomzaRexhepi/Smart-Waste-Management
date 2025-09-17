import random
from datetime import datetime
from dataclasses import dataclass

@dataclass
class SensorReading:
    bin_id: str
    timestamp: str
    sensor_type: str
    value: any
    unit: str = None

class UltrasonicSensor:
    """Simulates fill level detection"""
    def __init__(self, bin_id, capacity):
        self.bin_id = bin_id
        self.fill_level = 0.0
        self.capacity = capacity
        self.sensor_type = "fill_level"

    def measure(self):
        hour = datetime.now().hour
        base_increase = 1.0 if 6 <= hour <= 9 or 18 <= hour <= 22 else 0.3
        fill_increase = random.uniform(base_increase, base_increase * 3.0)

        if random.random() < 0.02:
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
    """Internal bin temperature"""
    def __init__(self, bin_id):
        self.bin_id = bin_id
        self.sensor_type = "temperature"
        self.base_temp = random.uniform(18.0, 25.0)

    def measure(self, fill_level):
        if fill_level > 70 and random.random() < 0.3:
            temp = self.base_temp + random.uniform(5.0, 15.0)
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
                "confidence": round(random.uniform(0.85, 0.98), 2)
            }
        )
