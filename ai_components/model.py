import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import pickle
import json
import uuid
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
from sklearn.ensemble import RandomForestRegressor, IsolationForest
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, mean_squared_error
import warnings
warnings.filterwarnings('ignore')

try:
    from kafka import KafkaProducer, KafkaConsumer
    from cassandra.cluster import Cluster
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False
    print("Kafka/Cassandra not available - running in simulation mode")

@dataclass
class PredictionAlert:
    bin_id: str
    alert_type: str  # 'critical_prediction', 'semi_warning', 'anomaly_detected'
    severity: str    # 'low', 'medium', 'high', 'critical'
    predicted_full_time: Optional[datetime]
    current_fill_level: float
    predicted_fill_level: float
    confidence: float
    message: str
    timestamp: datetime
    recommended_action: str

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

class WasteAIPredictor:
    def __init__(self, cassandra_host='127.0.0.1', kafka_brokers='localhost:9092'):
        self.fill_predictor = RandomForestRegressor(n_estimators=100, random_state=42)
        self.anomaly_detector = IsolationForest(contamination=0.1, random_state=42)
        self.scaler = StandardScaler()
        
        self.CRITICAL_THRESHOLD = 90.0
        self.WARNING_THRESHOLD = 75.0
        self.SEMI_WARNING_THRESHOLD = 60.0
        
        self.PREDICTION_HORIZONS = [1, 6, 12, 24, 48, 72]
        
        self.historical_data = {}
        self.model_trained = False
        
        if KAFKA_AVAILABLE:
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=kafka_brokers,
                    value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')
                )
                self.cluster = Cluster([cassandra_host])
                self.session = self.cluster.connect()
                self.session.execute("USE wastebin")
                print("Connected to Kafka and Cassandra")
            except Exception as e:
                print(f"Connection error: {e}")
                self.producer = None
                self.session = None
        else:
            self.producer = None
            self.session = None
    
    def extract_features(self, bin_id: str, sensor_data: List[Dict]) -> Optional[BinFeatures]:
        """Extract features from sensor data for AI model"""
        if not sensor_data:
            return None
            
        sensor_data = sorted(sensor_data, key=lambda x: x['timestamp'])
        latest = sensor_data[-1]
        
        current_fill = latest.get('fill_level', 0)
        temperature = latest.get('temperature', 20)
        humidity = latest.get('humidity', 50)
        weight = latest.get('weight', 0)
        
        # Time-based features
        now = datetime.now()
        hour_of_day = now.hour
        day_of_week = now.weekday()
        is_weekend = day_of_week >= 5
        season = (now.month - 1) // 3
        
        # Calculate fill rates
        fill_rate_1h = self._calculate_fill_rate(sensor_data, hours=1)
        fill_rate_6h = self._calculate_fill_rate(sensor_data, hours=6)
        fill_rate_24h = self._calculate_fill_rate(sensor_data, hours=24)
        
        # Historical averages
        avg_fill_last_week = self._get_average_fill_level(sensor_data, days=7)
        
        # Temperature trend (rising/falling)
        temperature_trend = self._calculate_temperature_trend(sensor_data)
        
        # Days since maintenance (mock calculation)
        days_since_maintenance = self._days_since_maintenance(bin_id)
        
        return BinFeatures(
            bin_id=bin_id,
            current_fill_level=current_fill,
            temperature=temperature,
            humidity=humidity,
            weight=weight,
            hour_of_day=hour_of_day,
            day_of_week=day_of_week,
            fill_rate_1h=fill_rate_1h,
            fill_rate_6h=fill_rate_6h,
            fill_rate_24h=fill_rate_24h,
            days_since_maintenance=days_since_maintenance,
            avg_fill_last_week=avg_fill_last_week,
            temperature_trend=temperature_trend,
            is_weekend=is_weekend,
            season=season
        )
    
    def _calculate_fill_rate(self, sensor_data: List[Dict], hours: int) -> float:
        """Calculate fill rate over specified hours"""
        if len(sensor_data) < 2:
            return 0.0
            
        cutoff_time = datetime.now() - timedelta(hours=hours)
        recent_data = [d for d in sensor_data 
                      if datetime.fromisoformat(d['timestamp'].replace('Z', '+00:00')) >= cutoff_time]
        
        if len(recent_data) < 2:
            return 0.0
            
        start_fill = recent_data[0].get('fill_level', 0)
        end_fill = recent_data[-1].get('fill_level', 0)
        
        return max(0, (end_fill - start_fill) / hours)
    
    def _get_average_fill_level(self, sensor_data: List[Dict], days: int) -> float:
        """Get average fill level over specified days"""
        cutoff_time = datetime.now() - timedelta(days=days)
        recent_data = [d for d in sensor_data 
                      if datetime.fromisoformat(d['timestamp'].replace('Z', '+00:00')) >= cutoff_time]
        
        if not recent_data:
            return 0.0
            
        return sum(d.get('fill_level', 0) for d in recent_data) / len(recent_data)
    
    def _calculate_temperature_trend(self, sensor_data: List[Dict]) -> float:
        """Calculate if temperature is trending up or down"""
        if len(sensor_data) < 5:
            return 0.0
            
        recent_temps = [d.get('temperature', 20) for d in sensor_data[-10:]]
        
        # Simple linear trend calculation
        x = np.arange(len(recent_temps))
        slope = np.polyfit(x, recent_temps, 1)[0]
        return slope
    
    def _days_since_maintenance(self, bin_id: str) -> int:
        """Mock calculation for days since maintenance"""
        # In real implementation, query maintenance_events table
        return np.random.randint(1, 45)
    
    def prepare_training_data(self, historical_sensor_data: Dict[str, List[Dict]]) -> Tuple[np.ndarray, np.ndarray]:
        """Prepare training data from historical sensor readings"""
        features_list = []
        targets_list = []
        
        for bin_id, sensor_readings in historical_sensor_data.items():
            if len(sensor_readings) < 50:  # Need sufficient data
                continue
                
            # Create training examples by sliding window
            for i in range(24, len(sensor_readings) - 24):  # 24 hours lookback, 24 hours prediction
                # Extract features from current state
                current_data = sensor_readings[:i+1]
                features = self.extract_features(bin_id, current_data)
                
                if features is None:
                    continue
                
                # Target is the fill level 24 hours later
                future_fill = sensor_readings[i + 24].get('fill_level', 0)
                
                features_array = self._features_to_array(features)
                features_list.append(features_array)
                targets_list.append(future_fill)
        
        if not features_list:
            return np.array([]), np.array([])
            
        return np.array(features_list), np.array(targets_list)
    
    def _features_to_array(self, features: BinFeatures) -> np.ndarray:
        """Convert BinFeatures to numpy array"""
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
        """Train the AI models on historical data"""
        print("Preparing training data...")
        X, y = self.prepare_training_data(historical_data)
        
        if len(X) == 0:
            print("No training data available")
            return
            
        print(f"Training on {len(X)} samples...")
        
        # Scale features
        X_scaled = self.scaler.fit_transform(X)
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X_scaled, y, test_size=0.2, random_state=42
        )
        
        # Train fill level predictor
        self.fill_predictor.fit(X_train, y_train)
        
        # Train anomaly detector
        self.anomaly_detector.fit(X_train)
        
        # Evaluate
        y_pred = self.fill_predictor.predict(X_test)
        mae = mean_absolute_error(y_test, y_pred)
        rmse = np.sqrt(mean_squared_error(y_test, y_pred))
        
        print(f"Model trained successfully!")
        print(f"Fill Level Prediction - MAE: {mae:.2f}%, RMSE: {rmse:.2f}%")
        
        self.model_trained = True
    
    def predict_fill_levels(self, bin_id: str, sensor_data: List[Dict]) -> Dict[int, float]:
        """Predict fill levels for different time horizons"""
        if not self.model_trained:
            print("Models not trained yet")
            return {}
            
        features = self.extract_features(bin_id, sensor_data)
        if features is None:
            return {}
            
        features_array = self._features_to_array(features).reshape(1, -1)
        features_scaled = self.scaler.transform(features_array)
        
        # Base prediction (24 hours)
        base_prediction = self.fill_predictor.predict(features_scaled)[0]
        
        # Scale predictions for different horizons based on fill rate
        predictions = {}
        current_fill = features.current_fill_level
        
        for horizon in self.PREDICTION_HORIZONS:
            if horizon <= 24:
                # Linear interpolation for shorter horizons
                predicted_fill = current_fill + (base_prediction - current_fill) * (horizon / 24)
            else:
                # Extrapolate for longer horizons (with diminishing returns)
                rate = (base_prediction - current_fill) / 24
                predicted_fill = current_fill + rate * horizon * (1 - horizon / 168)  # Week dampening
            
            predictions[horizon] = max(0, min(100, predicted_fill))
        
        return predictions
    
    def detect_anomalies(self, bin_id: str, sensor_data: List[Dict]) -> bool:
        """Detect if current sensor readings are anomalous"""
        if not self.model_trained:
            return False
            
        features = self.extract_features(bin_id, sensor_data)
        if features is None:
            return False
            
        features_array = self._features_to_array(features).reshape(1, -1)
        features_scaled = self.scaler.transform(features_array)
        
        anomaly_score = self.anomaly_detector.decision_function(features_scaled)[0]
        is_anomaly = self.anomaly_detector.predict(features_scaled)[0] == -1
        
        return is_anomaly
    
    def analyze_bin(self, bin_id: str, sensor_data: List[Dict]) -> List[PredictionAlert]:
        """Main analysis function that generates alerts"""
        alerts = []
        
        if not sensor_data:
            return alerts
            
        current_fill = sensor_data[-1].get('fill_level', 0)
        
        # Get predictions
        predictions = self.predict_fill_levels(bin_id, sensor_data)
        
        # Check for anomalies
        is_anomalous = self.detect_anomalies(bin_id, sensor_data)
        
        if is_anomalous:
            alerts.append(PredictionAlert(
                bin_id=bin_id,
                alert_type='anomaly_detected',
                severity='medium',
                predicted_full_time=None,
                current_fill_level=current_fill,
                predicted_fill_level=current_fill,
                confidence=0.8,
                message=f"Sensor readings show unusual patterns for bin {bin_id}",
                timestamp=datetime.now(),
                recommended_action="Inspect bin for potential issues"
            ))
        
        # Analyze predictions for different horizons
        for horizon_hours, predicted_fill in predictions.items():
            confidence = max(0.6, 1.0 - horizon_hours / 168)  # Confidence decreases with time
            
            # Critical prediction - will reach 90%+ soon
            if predicted_fill >= self.CRITICAL_THRESHOLD and current_fill < self.CRITICAL_THRESHOLD:
                estimated_time = datetime.now() + timedelta(hours=horizon_hours)
                alerts.append(PredictionAlert(
                    bin_id=bin_id,
                    alert_type='critical_prediction',
                    severity='high',
                    predicted_full_time=estimated_time,
                    current_fill_level=current_fill,
                    predicted_fill_level=predicted_fill,
                    confidence=confidence,
                    message=f"Bin {bin_id} predicted to reach {predicted_fill:.1f}% in {horizon_hours}h",
                    timestamp=datetime.now(),
                    recommended_action=f"Schedule collection within {max(1, horizon_hours-6)} hours"
                ))
                break  # Only send earliest critical alert
            
            # Semi-warning - trending towards full but not immediate
            elif (predicted_fill >= self.WARNING_THRESHOLD and 
                  current_fill < self.SEMI_WARNING_THRESHOLD and 
                  horizon_hours >= 24):
                
                estimated_time = datetime.now() + timedelta(hours=horizon_hours)
                alerts.append(PredictionAlert(
                    bin_id=bin_id,
                    alert_type='semi_warning',
                    severity='low',
                    predicted_full_time=estimated_time,
                    current_fill_level=current_fill,
                    predicted_fill_level=predicted_fill,
                    confidence=confidence,
                    message=f"Bin {bin_id} trending towards {predicted_fill:.1f}% in {horizon_hours}h",
                    timestamp=datetime.now(),
                    recommended_action="Monitor closely and plan collection route"
                ))
                break  # Only send one semi-warning
        
        return alerts
    
    def send_alert(self, alert: PredictionAlert):
        """Send alert through Kafka or print to console"""
        alert_data = {
            'bin_id': alert.bin_id,
            'alarm_type': alert.alert_type,
            'severity': alert.severity,
            'value': alert.predicted_fill_level,
            'timestamp': alert.timestamp.isoformat(),
            'message': alert.message,
            'confidence': alert.confidence,
            'recommended_action': alert.recommended_action
        }
        
        if self.producer:
            self.producer.send('alarms', value=alert_data)
            print(f"AI Alert sent: {alert.message}")
        else:
            print(f"AI ALERT - {alert.alert_type.upper()}: {alert.message}")
            print(f"  Severity: {alert.severity}")
            print(f"  Confidence: {alert.confidence:.1%}")
            print(f"  Action: {alert.recommended_action}")
    
    def process_sensor_reading(self, sensor_reading: Dict):
        """Process a new sensor reading and check for alerts"""
        bin_id = sensor_reading.get('bin_id')
        if not bin_id:
            return
            
        # Add to historical data
        if bin_id not in self.historical_data:
            self.historical_data[bin_id] = []
        
        self.historical_data[bin_id].append(sensor_reading)
        
        # Keep only last 1000 readings per bin
        if len(self.historical_data[bin_id]) > 1000:
            self.historical_data[bin_id] = self.historical_data[bin_id][-1000:]
        
        # Analyze if we have enough data and models are trained
        if len(self.historical_data[bin_id]) >= 24 and self.model_trained:
            alerts = self.analyze_bin(bin_id, self.historical_data[bin_id])
            
            for alert in alerts:
                self.send_alert(alert)
    
    def save_models(self, filepath: str):
        """Save trained models to disk"""
        if not self.model_trained:
            print("No trained models to save")
            return
            
        model_data = {
            'fill_predictor': self.fill_predictor,
            'anomaly_detector': self.anomaly_detector,
            'scaler': self.scaler,
            'model_trained': self.model_trained
        }
        
        with open(filepath, 'wb') as f:
            pickle.dump(model_data, f)
        
        print(f"Models saved to {filepath}")
    
    def load_models(self, filepath: str):
        """Load trained models from disk"""
        try:
            with open(filepath, 'rb') as f:
                model_data = pickle.load(f)
            
            self.fill_predictor = model_data['fill_predictor']
            self.anomaly_detector = model_data['anomaly_detector']
            self.scaler = model_data['scaler']
            self.model_trained = model_data['model_trained']
            
            print(f"Models loaded from {filepath}")
        except FileNotFoundError:
            print(f"Model file {filepath} not found")
        except Exception as e:
            print(f"Error loading models: {e}")

# Demo function to generate synthetic training data
def generate_synthetic_training_data(num_bins=10, days=30) -> Dict[str, List[Dict]]:
    """Generate synthetic historical data for training"""
    synthetic_data = {}
    
    for bin_id in [f"bin-{i:03d}" for i in range(num_bins)]:
        readings = []
        current_fill = np.random.uniform(10, 30)
        
        # Generate readings every hour for specified days
        for hour in range(days * 24):
            timestamp = datetime.now() - timedelta(hours=days*24-hour)
            
            # Simulate daily patterns
            hour_of_day = timestamp.hour
            if 6 <= hour_of_day <= 10 or 17 <= hour_of_day <= 21:
                fill_increase = np.random.uniform(1.5, 3.0)
            else:
                fill_increase = np.random.uniform(0.2, 1.0)
            
            # Random emptying events
            if current_fill > 85 and np.random.random() < 0.3:
                current_fill = np.random.uniform(5, 15)
            else:
                current_fill = min(100, current_fill + fill_increase)
            
            readings.append({
                'sensor_id': str(uuid.uuid4()),
                'bin_id': bin_id,
                'timestamp': timestamp.isoformat(),
                'fill_level': round(current_fill, 2),
                'temperature': round(20 + np.random.normal(0, 3) + (current_fill/100)*10, 1),
                'weight': round((current_fill/100) * np.random.uniform(50, 200), 2),
                'humidity': round(np.random.uniform(40, 80), 2),
                'status': 'normal'
            })
        
        synthetic_data[bin_id] = readings
    
    return synthetic_data

# Example usage
if __name__ == "__main__":
    # Initialize the AI predictor
    ai_predictor = WasteAIPredictor()
    
    print("Generating synthetic training data...")
    training_data = generate_synthetic_training_data(num_bins=20, days=45)
    
    print("Training AI models...")
    ai_predictor.train_models(training_data)
    
    # Save trained models
    ai_predictor.save_models('waste_ai_models.pkl')
    
    print("\n" + "="*60)
    print("AI WASTE PREDICTION SYSTEM READY")
    print("="*60)
    print("Features:")
    print("• Predictive fill level modeling (1-72 hour horizons)")
    print("• Anomaly detection for sensor malfunctions")
    print("• Semi-warnings for early intervention")
    print("• Critical alerts before bins overflow")
    print("• Confidence scoring for predictions")
    print("• Automatic model training and updates")
    print("="*60)
    
    # Demo with a test bin
    test_bin = "bin-001"
    test_readings = training_data[test_bin][-50:]  # Last 50 readings
    
    print(f"\nDemo analysis for {test_bin}:")
    alerts = ai_predictor.analyze_bin(test_bin, test_readings)
    
    if alerts:
        for alert in alerts:
            ai_predictor.send_alert(alert)
    else:
        print("No alerts generated - bin is operating normally")
    
    print("\nSystem ready to process real-time sensor data!")