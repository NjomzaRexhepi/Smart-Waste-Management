import os
os.environ["CASSANDRA_DRIVER_NO_EXTENSIONS"] = "1"
os.environ["CASSANDRA_DRIVER_EVENT_LOOP_MANAGER"] = "asyncio"

import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import uuid
from typing import Dict, List, Optional
from dataclasses import dataclass
from sklearn.ensemble import RandomForestRegressor, IsolationForest
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, mean_squared_error
import warnings
warnings.filterwarnings('ignore')

from cassandra.cluster import Cluster

# ---------------- DATA CLASSES ----------------
@dataclass
class BinFeatures:
    bin_id: str
    current_fill_level: float
    temperature: float
    humidity: float
    weight: float
    hour_of_day: int
    day_of_week: int
    fill_rate_1h: float
    fill_rate_6h: float
    fill_rate_24h: float
    days_since_maintenance: int
    avg_fill_last_week: float
    temperature_trend: float
    is_weekend: bool
    season: int  # 0=Winter, 1=Spring, 2=Summer, 3=Fall

# ---------------- AI PREDICTOR ----------------
class WasteAIPredictor:
    def __init__(self, cassandra_host='127.0.0.1', cassandra_port=9042, keyspace='wastebin'):
        self.fill_predictor = RandomForestRegressor(n_estimators=100, random_state=42)
        self.anomaly_detector = IsolationForest(contamination=0.1, random_state=42)
        self.scaler = StandardScaler()
        self.CRITICAL_THRESHOLD = 90.0
        self.WARNING_THRESHOLD = 75.0
        self.SEMI_WARNING_THRESHOLD = 60.0
        self.PREDICTION_HORIZONS = [1, 6, 12, 24]
        self.model_trained = False

        # Cassandra Connection
        try:
            self.cluster = Cluster([cassandra_host], port=cassandra_port)
            self.session = self.cluster.connect('wastebin')
            print(f"✅ Connected to Cassandra at {cassandra_host}:{cassandra_port}, keyspace '{keyspace}'")
        except Exception as e:
            print(f"❌ Cassandra connection error: {e}")
            self.session = None

    # ---------------- FETCH DATA ----------------
    def fetch_data_from_cassandra(self, limit: int = 5000) -> Dict[str, List[Dict]]:
        if not self.session:
            print("Cassandra session not available")
            return {}

        try:
            query = f"SELECT bin_id, timestamp, fill_level, temperature, humidity, weight FROM sensor_data LIMIT {limit};"
            rows = self.session.execute(query)

            sensor_data: Dict[str, List[Dict]] = {}
            for row in rows:
                if row.bin_id not in sensor_data:
                    sensor_data[row.bin_id] = []
                sensor_data[row.bin_id].append({
                    'sensor_id': str(uuid.uuid4()),
                    'bin_id': row.bin_id,
                    'timestamp': str(row.timestamp),
                    'fill_level': float(row.fill_level) if row.fill_level else 0.0,
                    'temperature': float(row.temperature) if row.temperature else 20.0,
                    'humidity': float(row.humidity) if row.humidity else 50.0,
                    'weight': float(row.weight) if row.weight else 0.0,
                })
            print(f"Fetched {sum(len(v) for v in sensor_data.values())} records from Cassandra")
            return sensor_data

        except Exception as e:
            print(f"Error fetching data from Cassandra: {e}")
            return {}

    # ---------------- FEATURE EXTRACTION ----------------
    def extract_features(self, bin_id: str, sensor_data: List[Dict]) -> Optional[BinFeatures]:
        if not sensor_data:
            return None
        sensor_data = sorted(sensor_data, key=lambda x: x['timestamp'])
        latest = sensor_data[-1]

        now = datetime.now()
        hour_of_day = now.hour
        day_of_week = now.weekday()
        is_weekend = day_of_week >= 5
        season = (now.month - 1) // 3

        return BinFeatures(
            bin_id=bin_id,
            current_fill_level=latest.get('fill_level', 0),
            temperature=latest.get('temperature', 20),
            humidity=latest.get('humidity', 50),
            weight=latest.get('weight', 0),
            hour_of_day=hour_of_day,
            day_of_week=day_of_week,
            fill_rate_1h=self._calculate_fill_rate(sensor_data, 1),
            fill_rate_6h=self._calculate_fill_rate(sensor_data, 6),
            fill_rate_24h=self._calculate_fill_rate(sensor_data, 24),
            days_since_maintenance=np.random.randint(1,45),
            avg_fill_last_week=self._get_average_fill_level(sensor_data, 7),
            temperature_trend=self._calculate_temperature_trend(sensor_data),
            is_weekend=is_weekend,
            season=season
        )

    def _calculate_fill_rate(self, sensor_data: List[Dict], hours: int) -> float:
        if len(sensor_data) < 2: return 0.0
        cutoff_time = datetime.now() - timedelta(hours=hours)
        recent_data = [d for d in sensor_data if datetime.fromisoformat(d['timestamp']) >= cutoff_time]
        if len(recent_data) < 2: return 0.0
        return max(0, (recent_data[-1]['fill_level'] - recent_data[0]['fill_level']) / hours)

    def _get_average_fill_level(self, sensor_data: List[Dict], days: int) -> float:
        cutoff_time = datetime.now() - timedelta(days=days)
        recent_data = [d for d in sensor_data if datetime.fromisoformat(d['timestamp']) >= cutoff_time]
        if not recent_data: return 0.0
        return sum(d['fill_level'] for d in recent_data) / len(recent_data)

    def _calculate_temperature_trend(self, sensor_data: List[Dict]) -> float:
        if len(sensor_data) < 5: return 0.0
        temps = [d.get('temperature', 20) for d in sensor_data[-10:]]
        x = np.arange(len(temps))
        return np.polyfit(x, temps, 1)[0]

    # ---------------- TRAINING ----------------
    def prepare_training_data(self, historical_sensor_data: Dict[str, List[Dict]]):
        X_list, y_list = [], []
        for bin_id, readings in historical_sensor_data.items():
            if len(readings) < 50: continue
            for i in range(24, len(readings)-24):
                features = self.extract_features(bin_id, readings[:i+1])
                if not features: continue
                X_list.append(self._features_to_array(features))
                y_list.append(readings[i+24].get('fill_level', 0))
        return np.array(X_list), np.array(y_list)

    def _features_to_array(self, features: BinFeatures) -> np.ndarray:
        return np.array([
            features.current_fill_level,
            features.temperature,
            features.humidity,
            features.weight,
            features.hour_of_day,
            features.day_of_week,
            features.fill_rate_1h,
            features.fill_rate_6h,
            features.fill_rate_24h,
            features.days_since_maintenance,
            features.avg_fill_last_week,
            features.temperature_trend,
            int(features.is_weekend),
            features.season
        ])

    def train_models(self, historical_data: Dict[str, List[Dict]]):
        X, y = self.prepare_training_data(historical_data)
        if len(X) == 0: return
        X_scaled = self.scaler.fit_transform(X)
        X_train, X_test, y_train, y_test = train_test_split(
            X_scaled, y, test_size=0.2, random_state=42
        )
        self.fill_predictor.fit(X_train, y_train)
        self.anomaly_detector.fit(X_train)
        y_pred = self.fill_predictor.predict(X_test)
        print(f"MAE: {mean_absolute_error(y_test, y_pred):.2f}, RMSE: {np.sqrt(mean_squared_error(y_test, y_pred)):.2f}")
        self.model_trained = True

    # ---------------- ADVANCED FUTURE PREDICTION ----------------
    def predict_future_fill_advanced(self, bin_id: str, sensor_data: List[Dict], horizons: List[int] = None) -> Dict[int, Dict]:
        if not horizons:
            horizons = self.PREDICTION_HORIZONS

        results = {}
        features = self.extract_features(bin_id, sensor_data)
        if not features:
            return {h: {"predicted_fill": None, "confidence": None} for h in horizons}

        base_features = self._features_to_array(features)
        # Predict for 1h only
        horizon_features = base_features.copy()
        horizon_features[0] = features.current_fill_level + features.fill_rate_1h  # 1h fill
        horizon_features[4] = (features.hour_of_day + 1) % 24
        horizon_features[5] = (features.day_of_week + (features.hour_of_day + 1)//24) % 7

        X_scaled = self.scaler.transform(horizon_features.reshape(1, -1))
        predicted_1h = self.fill_predictor.predict(X_scaled)[0]
        predicted_1h = min(100, max(0, predicted_1h))
        confidence = float(np.std([tree.predict(X_scaled)[0] for tree in self.fill_predictor.estimators_]))

        for h in horizons:
            predicted = min(100, predicted_1h * h)  # scale by horizon
            results[h] = {"predicted_fill": round(predicted, 2), "confidence": round(confidence, 2)}

        return results


# ---------------- MAIN ----------------
if __name__ == "__main__":
    predictor = WasteAIPredictor()
    data = predictor.fetch_data_from_cassandra(limit=5000)
    predictor.train_models(data)

    for bin_id, readings in data.items():
        future_preds = predictor.predict_future_fill_advanced(bin_id, readings)
        print(f"Bin {bin_id} predictions:")
        for h, pred in future_preds.items():
            print(f"  +{h}h → Fill: {pred['predicted_fill']}%, Confidence: {pred['confidence']}")
