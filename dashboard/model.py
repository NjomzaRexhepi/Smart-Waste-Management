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
    horizon: int = 0

# ---------------- AI PREDICTOR ----------------
class WasteAIPredictor:
    def __init__(self, cassandra_host='127.0.0.1', cassandra_port=9042, keyspace='wastebin'):
        self.fill_predictor = RandomForestRegressor(
            n_estimators=200, max_depth=10, min_samples_split=5, max_features='sqrt', random_state=42
        )
        self.anomaly_detector = IsolationForest(contamination=0.1, random_state=42)
        self.scaler = StandardScaler()
        self.CRITICAL_THRESHOLD = 90.0
        self.WARNING_THRESHOLD = 75.0
        self.SEMI_WARNING_THRESHOLD = 60.0
        self.PREDICTION_HORIZONS = [1, 6, 12, 24]
        self.model_trained = False

        try:
            self.cluster = Cluster([cassandra_host], port=cassandra_port)
            self.session = self.cluster.connect(keyspace)
            print(f"Connected to Cassandra at {cassandra_host}:{cassandra_port}, keyspace '{keyspace}'")
        except Exception as e:
            print(f"Cassandra connection error: {e}")
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
                ts = row.timestamp if isinstance(row.timestamp, str) else str(row.timestamp)
                bin_id = str(row.bin_id)
                if bin_id not in sensor_data:
                    sensor_data[bin_id] = []
                sensor_data[bin_id].append({
                    'sensor_id': str(uuid.uuid4()),
                    'bin_id': bin_id,
                    'timestamp': ts,
                    'fill_level': float(row.fill_level) if row.fill_level is not None else 0.0,
                    'temperature': float(row.temperature) if row.temperature is not None else 20.0,
                    'humidity': float(row.humidity) if row.humidity is not None else 50.0,
                    'weight': float(row.weight) if row.weight is not None else 0.0,
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
        try:
            sensor_data = sorted(sensor_data, key=lambda x: datetime.fromisoformat(x['timestamp']))
        except Exception:
            sensor_data = sorted(sensor_data, key=lambda x: x['timestamp'])

        latest = sensor_data[-1]

        now = datetime.now()
        hour_of_day = now.hour
        day_of_week = now.weekday()
        is_weekend = day_of_week >= 5
        season = (now.month - 1) // 3

        fill_rate_1h = self._calculate_fill_rate(sensor_data, 1, prefer_window_hours=12)
        fill_rate_6h = self._calculate_fill_rate(sensor_data, 6, prefer_window_hours=24)
        fill_rate_24h = self._calculate_fill_rate(sensor_data, 24, prefer_window_hours=48)

        return BinFeatures(
            bin_id=bin_id,
            current_fill_level=latest.get('fill_level', 0.0),
            temperature=latest.get('temperature', 20.0),
            humidity=latest.get('humidity', 50.0),
            weight=latest.get('weight', 0.0),
            hour_of_day=hour_of_day,
            day_of_week=day_of_week,
            fill_rate_1h=fill_rate_1h,
            fill_rate_6h=fill_rate_6h,
            fill_rate_24h=fill_rate_24h,
            days_since_maintenance=np.random.randint(1,45),
            avg_fill_last_week=self._get_average_fill_level(sensor_data, 7),
            temperature_trend=self._calculate_temperature_trend(sensor_data),
            is_weekend=is_weekend,
            season=season,
            horizon=0
        )

    def _calculate_fill_rate(self, sensor_data: List[Dict], hours: int, prefer_window_hours: int = 12) -> float:
        prefer_cutoff = datetime.now() - timedelta(hours=prefer_window_hours)
        cutoff_time = max(datetime.now() - timedelta(hours=hours), prefer_cutoff)

        try:
            recent_data = [d for d in sensor_data if datetime.fromisoformat(d['timestamp']) >= cutoff_time]
        except Exception:
            recent_data = [d for d in sensor_data if d['timestamp'] >= cutoff_time.isoformat()]

        if len(recent_data) < 3:
            try:
                wide_cutoff = datetime.now() - timedelta(hours=48)
                recent_data = [d for d in sensor_data if datetime.fromisoformat(d['timestamp']) >= wide_cutoff]
            except Exception:
                recent_data = sensor_data[-48:] if len(sensor_data) >= 1 else []

        if len(recent_data) < 2:
            return 0.0

        try:
            t0 = datetime.fromisoformat(recent_data[0]['timestamp'])
            times = [(datetime.fromisoformat(d['timestamp']) - t0).total_seconds() / 3600.0 for d in recent_data]
        except Exception:
            times_dt = pd.to_datetime([d['timestamp'] for d in recent_data])
            t0 = times_dt[0]
            times = [(t - t0).total_seconds() / 3600.0 for t in times_dt]

        levels = [float(d.get('fill_level', 0.0)) for d in recent_data]
        if len(set(times)) == 1:
            return 0.0

        try:
            slope, intercept = np.polyfit(times, levels, 1)
        except Exception:
            delta_hours = times[-1] - times[0]
            if delta_hours <= 0:
                return 0.0
            slope = (levels[-1] - levels[0]) / delta_hours

        slope = max(-1.0, min(slope, 5.0))
        return float(slope)

    def _get_average_fill_level(self, sensor_data: List[Dict], days: int) -> float:
        cutoff_time = datetime.now() - timedelta(days=days)
        try:
            recent_data = [d for d in sensor_data if datetime.fromisoformat(d['timestamp']) >= cutoff_time]
        except Exception:
            recent_data = [d for d in sensor_data if d['timestamp'] >= cutoff_time.isoformat()]

        if not recent_data:
            return 0.0
        return float(sum(d.get('fill_level', 0.0) for d in recent_data) / len(recent_data))

    def _calculate_temperature_trend(self, sensor_data: List[Dict]) -> float:
        if len(sensor_data) < 5:
            return 0.0
        temps = [float(d.get('temperature', 20.0)) for d in sensor_data[-10:]]
        x = np.arange(len(temps))
        try:
            return float(np.polyfit(x, temps, 1)[0])
        except Exception:
            return 0.0

    # ---------------- TRAINING ----------------
    def prepare_training_data(self, historical_sensor_data: Dict[str, List[Dict]]):
        X_list, y_list = [], []
        for bin_id, readings in historical_sensor_data.items():
            if len(readings) < 50:
                continue
            try:
                readings = sorted(readings, key=lambda x: datetime.fromisoformat(x['timestamp']))
            except Exception:
                readings = sorted(readings, key=lambda x: x['timestamp'])
            for i in range(24, len(readings) - max(self.PREDICTION_HORIZONS)):
                for h in self.PREDICTION_HORIZONS:
                    if i + h >= len(readings):
                        continue
                    features = self.extract_features(bin_id, readings[:i+1])
                    if not features:
                        continue
                    features.horizon = h
                    X_list.append(self._features_to_array(features))
                    y_list.append(float(readings[i + h].get('fill_level', 0.0)))
        X = np.array(X_list) if X_list else np.array([])
        y = np.array(y_list) if y_list else np.array([])
        if len(X) > 0:
            print(f"Training data shape: {X.shape}, Feature std: {np.std(X, axis=0)}")
        return X, y

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
            features.season,
            features.horizon
        ], dtype=float)

    def train_models(self, historical_data: Dict[str, List[Dict]]):
        X, y = self.prepare_training_data(historical_data)
        if len(X) == 0:
            print("No training data available.")
            return
        X_scaled = self.scaler.fit_transform(X)
        X_train, X_test, y_train, y_test = train_test_split(
            X_scaled, y, test_size=0.2, random_state=42
        )
        self.fill_predictor.fit(X_train, y_train)
        self.anomaly_detector.fit(X_train)
        y_pred = self.fill_predictor.predict(X_test)
        print(f"MAE: {mean_absolute_error(y_test, y_pred):.2f}, RMSE: {np.sqrt(mean_squared_error(y_test, y_pred)):.2f}")
        print("Feature importances:", self.fill_predictor.feature_importances_)
        self.model_trained = True

    # ---------------- ADVANCED FUTURE PREDICTION ----------------
    def predict_future_fill_advanced(self, bin_id: str, sensor_data: List[Dict], horizons: List[int] = None) -> Dict[int, Dict]:
        if not horizons:
            horizons = self.PREDICTION_HORIZONS

        results = {}
        features = self.extract_features(bin_id, sensor_data)
        if not features:
            return {h: {"predicted_fill": None, "confidence": None} for h in horizons}

        current_level = float(features.current_fill_level)
        base_features = self._features_to_array(features)
        slope_from_regression = self._calculate_fill_rate(sensor_data, 12, prefer_window_hours=24)

        prev_fill = current_level
        for h in sorted(horizons):
            horizon_features = base_features.copy()
            # Update fill level
            horizon_features[0] = min(100.0, current_level + slope_from_regression * h)
            # Update time
            horizon_features[4] = (features.hour_of_day + h) % 24
            horizon_features[5] = (features.day_of_week + (features.hour_of_day + h)//24) % 7
            # Simulate dynamic fill rates
            horizon_features[6] = self._calculate_fill_rate(sensor_data, 1, prefer_window_hours=max(12, h))
            horizon_features[7] = self._calculate_fill_rate(sensor_data, 6, prefer_window_hours=max(24, h))
            horizon_features[8] = self._calculate_fill_rate(sensor_data, 24, prefer_window_hours=max(48, h))
            # Simulate temperature/humidity changes
            horizon_features[1] = features.temperature + features.temperature_trend * h + np.random.normal(0, 0.5)
            horizon_features[2] = min(100, max(0, features.humidity + np.random.normal(0, 2)))
            # Set horizon
            horizon_features[-1] = h

            X_scaled = self.scaler.transform(horizon_features.reshape(1, -1))
            tree_preds = np.array([tree.predict(X_scaled)[0] for tree in self.fill_predictor.estimators_])
            predicted = float(np.mean(tree_preds))
            lower_bound = float(np.percentile(tree_preds, 10))
            upper_bound = float(np.percentile(tree_preds, 90))
            confidence = (upper_bound - lower_bound) / 2

            # Enforce non-decreasing
            predicted = max(predicted, prev_fill)

            results[h] = {
                "predicted_fill": round(min(100, max(0, predicted)), 2),
                "confidence": round(confidence, 2)
            }
            prev_fill = results[h]["predicted_fill"]

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