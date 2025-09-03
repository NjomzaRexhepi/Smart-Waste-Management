import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
import json
import random
from typing import Dict, List, Optional
import warnings
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

warnings.filterwarnings('ignore')

# Set page configuration
st.set_page_config(
    page_title="IoT Waste Management System",
    page_icon="🗑️",
    layout="wide",
    initial_sidebar_state="expanded"
)


class IoTWasteManagementSystem:
    def __init__(self, num_bins: int = 50, simulation_days: int = 30):
        self.num_bins = num_bins
        self.simulation_days = simulation_days

        self.bins_df = self._generate_bins_metadata()
        self.sensor_data_df = pd.DataFrame(columns=[
            'timestamp', 'bin_id', 'district', 'bin_type', 'capacity_kg',
            'fill_level_percent', 'temperature_celsius', 'humidity_percent',
            'pressure_hpa', 'sound_level_db', 'co2_ppm', 'battery_percent',
            'ping_ms', 'motion_detected', 'device_status',
            'gps_latitude', 'gps_longitude'
        ])

        self.citizen_reports_df = pd.DataFrame()
        self.maintenance_logs_df = pd.DataFrame()
        self.collection_routes_df = pd.DataFrame()

    def _generate_bins_metadata(self) -> pd.DataFrame:
        """Generate metadata for waste bins"""
        districts = ["Center", "Industrial", "Residential", "Commercial"]
        bin_types = ["street", "residential", "commercial", "overflow"]
        capacities = [60, 120, 240, 500]

        bins_data = []
        for i in range(self.num_bins):
            bins_data.append({
                'bin_id': f'bin-{i:03d}',
                'type': random.choice(bin_types),
                'capacity_kg': random.choice(capacities),
                'district': random.choice(districts),
                'installation_date': datetime.now() - timedelta(days=random.randint(30, 730)),
                'last_maintenance': datetime.now() - timedelta(days=random.randint(1, 180)),
                'latitude': 42.66 + random.uniform(-0.05, 0.05),
                'longitude': 21.17 + random.uniform(-0.05, 0.05),
                'status': 'active'
            })

        return pd.DataFrame(bins_data)

    def generate_sensor_data(self, hours: int = 24) -> pd.DataFrame:
        """Generate realistic sensor data for specified time period"""
        sensor_data = []

        # Generate timestamps (every 15 minutes)
        end_time = datetime.now()
        start_time = end_time - timedelta(hours=hours)
        timestamps = pd.date_range(start=start_time, end=end_time, freq='15min')

        for timestamp in timestamps:
            for _, bin_info in self.bins_df.iterrows():
                bin_id = bin_info['bin_id']

                fill_level = self._simulate_fill_level(bin_id, timestamp)
                temperature = self._simulate_temperature(fill_level)

                # Additional sensor data
                humidity = round(random.uniform(10, 90), 1)
                pressure = round(random.uniform(980, 1050), 1)
                sound_level = round(random.uniform(30, 100), 1)
                co2_ppm = round(random.uniform(400, 2000))
                battery_level = max(20, 100 - random.uniform(0, 0.5))
                ping_ms = round(random.uniform(10, 300), 1)
                motion_detected = random.choice([True, False])
                device_status = random.choice(["OK", "ERROR", "OFFLINE"])

                gps_lat = bin_info['latitude'] + random.uniform(-0.0001, 0.0001)
                gps_lon = bin_info['longitude'] + random.uniform(-0.0001, 0.0001)

                sensor_data.append({
                    'timestamp': timestamp,
                    'bin_id': bin_id,
                    'district': bin_info['district'],
                    'bin_type': bin_info['type'],
                    'capacity_kg': bin_info['capacity_kg'],
                    'fill_level_percent': fill_level,
                    'temperature_celsius': temperature,
                    'humidity_percent': humidity,
                    'pressure_hpa': pressure,
                    'sound_level_db': sound_level,
                    'co2_ppm': co2_ppm,
                    'battery_percent': battery_level,
                    'ping_ms': ping_ms,
                    'motion_detected': motion_detected,
                    'device_status': device_status,
                    'gps_latitude': gps_lat,
                    'gps_longitude': gps_lon
                })

        new_data = pd.DataFrame(sensor_data)
        self.sensor_data_df = pd.concat([self.sensor_data_df, new_data], ignore_index=True)
        return new_data

    def _simulate_fill_level(self, bin_id: str, timestamp: datetime) -> float:
        base_rate = 2.0

        hour = timestamp.hour
        weekday = timestamp.weekday()

        if 7 <= hour <= 19:
            time_multiplier = 1.5
        else:
            time_multiplier = 0.5

        if weekday >= 5:
            time_multiplier *= 1.3

        district = self.bins_df[self.bins_df['bin_id'] == bin_id]['district'].iloc[0]
        district_multipliers = {
            'Commercial': 2.0,
            'Industrial': 1.5,
            'Center': 1.8,
            'Residential': 1.0
        }

        fill_rate = base_rate * time_multiplier * district_multipliers.get(district, 1.0)

        if random.random() < 0.02:
            current_fill = 5.0
        else:
            previous_data = self.sensor_data_df[
                (self.sensor_data_df['bin_id'] == bin_id) &
                (self.sensor_data_df['timestamp'] < timestamp)
                ]

            if not previous_data.empty:
                last_fill = previous_data.iloc[-1]['fill_level_percent']
                current_fill = min(100, last_fill + random.uniform(0, fill_rate * 0.25))
            else:
                current_fill = random.uniform(10, 40)

        return round(current_fill, 1)

    def _simulate_temperature(self, fill_level: float) -> float:
        """Simulate temperature based on fill level and organic waste"""
        base_temp = 20 + random.uniform(-3, 3)

        temp_increase = (fill_level / 100) * random.uniform(2, 8)

        if random.random() < 0.15:
            temp_increase += random.uniform(5, 15)

        return round(base_temp + temp_increase, 1)

    def generate_citizen_reports(self, num_reports: int = 20) -> pd.DataFrame:
        """Generate citizen report data"""
        issue_types = [
            "Overflowing bin", "Bad odor", "Recycling contamination",
            "Damaged bin", "Illegal dumping", "Missed collection",
            "Bin blocking path", "Vandalism", "Pest problem"
        ]

        priorities = ["Low", "Medium", "High", "Critical"]

        reports_data = []
        for i in range(num_reports):
            report_time = datetime.now() - timedelta(days=random.uniform(0, 7))
            bin_id = random.choice(self.bins_df['bin_id'].tolist())
            bin_info = self.bins_df[self.bins_df['bin_id'] == bin_id].iloc[0]

            reports_data.append({
                'report_id': f'report-{datetime.now().strftime("%Y%m%d")}-{i:04d}',
                'timestamp': report_time,
                'bin_id': bin_id,
                'district': bin_info['district'],
                'issue_type': random.choice(issue_types),
                'priority': random.choice(priorities),
                'reporter_id': f'citizen-{random.randint(1000, 9999)}',
                'description': f'Issue reported for bin {bin_id}',
                'status': random.choice(['Pending', 'In Progress', 'Resolved']),
                'resolution_time_hours': random.uniform(0.5, 6) if random.random() > 0.3 else None
            })

        self.citizen_reports_df = pd.DataFrame(reports_data)
        return self.citizen_reports_df

    def analyze_bin_performance(self) -> Dict:
        """Analyze bin performance metrics"""
        if self.sensor_data_df.empty:
            return {"error": "No sensor data available"}

        latest_data = self.sensor_data_df.groupby('bin_id').last()

        analysis = {
            'total_bins': len(self.bins_df),
            'bins_needing_collection': len(latest_data[latest_data['fill_level_percent'] >= 80]),
            'critical_bins': len(latest_data[latest_data['fill_level_percent'] >= 95]),
            'average_fill_level': latest_data['fill_level_percent'].mean(),
            'average_temperature': latest_data['temperature_celsius'].mean(),
            'bins_with_high_temp': len(latest_data[latest_data['temperature_celsius'] > 35]),
            'low_battery_bins': len(latest_data[latest_data['battery_percent'] < 20]),
            'offline_bins': len(latest_data[latest_data['device_status'] == 'OFFLINE']),
            'bins_by_district': latest_data.groupby('district')['fill_level_percent'].agg(['count', 'mean']).to_dict()
        }

        return analysis

    def detect_anomalies(self) -> pd.DataFrame:
        if self.sensor_data_df.empty:
            return pd.DataFrame()

        anomalies = []

        for bin_id, bin_data in self.sensor_data_df.groupby('bin_id'):
            bin_data = bin_data.sort_values('timestamp')

            temp_mean = bin_data['temperature_celsius'].mean()
            temp_std = bin_data['temperature_celsius'].std()
            temp_threshold = temp_mean + 2 * temp_std

            temp_anomalies = bin_data[bin_data['temperature_celsius'] > temp_threshold]
            for _, row in temp_anomalies.iterrows():
                anomalies.append({
                    'bin_id': bin_id,
                    'timestamp': row['timestamp'],
                    'anomaly_type': 'High Temperature',
                    'value': row['temperature_celsius'],
                    'threshold': temp_threshold,
                    'severity': 'High' if row['temperature_celsius'] > 40 else 'Medium'
                })

            fill_diff = bin_data['fill_level_percent'].diff().abs()
            sudden_drops = bin_data[fill_diff > 30]

            for _, row in sudden_drops.iterrows():
                anomalies.append({
                    'bin_id': bin_id,
                    'timestamp': row['timestamp'],
                    'anomaly_type': 'Sudden Fill Change',
                    'value': row['fill_level_percent'],
                    'threshold': 30,
                    'severity': 'Medium'
                })

        return pd.DataFrame(anomalies)

    def optimize_collection_routes(self) -> pd.DataFrame:
        if self.sensor_data_df.empty:
            return pd.DataFrame()

        latest_data = self.sensor_data_df.groupby('bin_id').last()

        bins_needing_collection = latest_data[latest_data['fill_level_percent'] >= 70].copy()

        if bins_needing_collection.empty:
            return pd.DataFrame()

        bins_needing_collection['priority_score'] = (
                bins_needing_collection['fill_level_percent'] * 0.6 +  # Fill level weight
                (bins_needing_collection['temperature_celsius'] - 20) * 0.2 +  # Temperature weight
                bins_needing_collection['district'].map({
                    'Commercial': 30, 'Center': 25, 'Industrial': 20, 'Residential': 15
                }) * 0.2
        )

        route_data = bins_needing_collection.sort_values('priority_score', ascending=False).reset_index()

        route_data['route_order'] = range(1, len(route_data) + 1)
        route_data['estimated_time_minutes'] = route_data['route_order'] * 15  # 15 min per bin
        route_data['cumulative_distance_km'] = np.cumsum(np.full(len(route_data), 2.5))  # Avg 2.5km between bins

        self.collection_routes_df = route_data
        return route_data[['bin_id', 'district', 'fill_level_percent', 'priority_score',
                           'route_order', 'estimated_time_minutes', 'cumulative_distance_km']]

    def generate_reports(self) -> Dict:
        reports = {}

        if not self.sensor_data_df.empty:
            latest_data = self.sensor_data_df.groupby('bin_id').last()

            performance_report = {
                'timestamp': datetime.now().isoformat(),
                'total_bins': len(self.bins_df),
                'active_bins': len(latest_data[latest_data['device_status'] == 'OK']),
                'average_fill_level': f"{latest_data['fill_level_percent'].mean():.1f}%",
                'bins_requiring_collection': len(latest_data[latest_data['fill_level_percent'] >= 80]),
                'critical_bins': len(latest_data[latest_data['fill_level_percent'] >= 95]),
                'average_temperature': f"{latest_data['temperature_celsius'].mean():.1f}°C",
                'high_temperature_alerts': len(latest_data[latest_data['temperature_celsius'] > 35]),
                'low_battery_warnings': len(latest_data[latest_data['battery_percent'] < 20]),
                'offline_devices': len(latest_data[latest_data['device_status'] == 'OFFLINE'])
            }
            reports['performance'] = performance_report

        if not self.citizen_reports_df.empty:
            citizen_summary = {
                'total_reports': len(self.citizen_reports_df),
                'pending_reports': len(self.citizen_reports_df[self.citizen_reports_df['status'] == 'Pending']),
                'average_resolution_time': f"{self.citizen_reports_df['resolution_time_hours'].mean():.1f} hours",
                'most_common_issues': self.citizen_reports_df['issue_type'].value_counts().head().to_dict(),
                'reports_by_district': self.citizen_reports_df.groupby('district').size().to_dict()
            }
            reports['citizen_reports'] = citizen_summary

        if not self.collection_routes_df.empty:
            route_summary = {
                'bins_in_route': len(self.collection_routes_df),
                'total_estimated_time': f"{self.collection_routes_df['estimated_time_minutes'].max()} minutes",
                'total_distance': f"{self.collection_routes_df['cumulative_distance_km'].max():.1f} km",
                'high_priority_bins': len(self.collection_routes_df[self.collection_routes_df['priority_score'] > 80]),
                'districts_covered': self.collection_routes_df['district'].nunique()
            }
            reports['route_optimization'] = route_summary

        return reports

    def visualize_data(self):
        if self.sensor_data_df.empty:
            st.warning("No data to visualize. Generate sensor data first.")
            return

        tab1, tab2, tab3, tab4 = st.tabs(["Overview", "District Analysis", "Time Analysis", "Anomalies"])

        with tab1:
            st.subheader("System Overview")
            col1, col2, col3 = st.columns(3)

            latest_data = self.sensor_data_df.groupby('bin_id').last()

            with col1:
                fig = px.pie(values=latest_data['device_status'].value_counts().values,
                             names=latest_data['device_status'].value_counts().index,
                             title="Device Status Distribution")
                st.plotly_chart(fig, use_container_width=True)

            with col2:
                fig = px.histogram(latest_data, x='fill_level_percent',
                                   title="Fill Level Distribution", nbins=20)
                fig.add_vline(x=80, line_dash="dash", line_color="red", annotation_text="Collection Threshold")
                st.plotly_chart(fig, use_container_width=True)

            with col3:
                fig = px.scatter(latest_data, x='fill_level_percent', y='temperature_celsius',
                                 color='district', title="Temperature vs Fill Level",
                                 labels={'fill_level_percent': 'Fill Level (%)',
                                         'temperature_celsius': 'Temperature (°C)'})
                st.plotly_chart(fig, use_container_width=True)

        with tab2:
            st.subheader("District Analysis")

            col1, col2 = st.columns(2)

            with col1:
                district_stats = latest_data.groupby('district').agg({
                    'fill_level_percent': 'mean',
                    'temperature_celsius': 'mean',
                    'battery_percent': 'mean'
                }).reset_index()

                fig = px.bar(district_stats, x='district', y='fill_level_percent',
                             title="Average Fill Level by District",
                             labels={'fill_level_percent': 'Fill Level (%)', 'district': 'District'})
                st.plotly_chart(fig, use_container_width=True)

            with col2:
                fig = px.box(latest_data, x='district', y='fill_level_percent',
                             title="Fill Level Distribution by District",
                             labels={'fill_level_percent': 'Fill Level (%)', 'district': 'District'})
                st.plotly_chart(fig, use_container_width=True)

        with tab3:
            st.subheader("Time Analysis")

            sample_bins = self.sensor_data_df['bin_id'].unique()[:3]
            time_data = self.sensor_data_df[self.sensor_data_df['bin_id'].isin(sample_bins)]

            fig = px.line(time_data, x='timestamp', y='fill_level_percent', color='bin_id',
                          title="Fill Level Trends Over Time",
                          labels={'fill_level_percent': 'Fill Level (%)', 'timestamp': 'Time'})
            st.plotly_chart(fig, use_container_width=True)

            hourly_data = self.sensor_data_df.copy()
            hourly_data['hour'] = hourly_data['timestamp'].dt.hour
            hourly_avg = hourly_data.groupby('hour')['fill_level_percent'].mean().reset_index()

            fig = px.line(hourly_avg, x='hour', y='fill_level_percent',
                          title="Average Fill Level by Hour of Day",
                          labels={'fill_level_percent': 'Fill Level (%)', 'hour': 'Hour'})
            st.plotly_chart(fig, use_container_width=True)

        with tab4:
            st.subheader("Anomaly Detection")

            anomalies = self.detect_anomalies()
            if not anomalies.empty:
                st.dataframe(anomalies[['bin_id', 'anomaly_type', 'severity', 'timestamp']])

                # Plot anomalies on timeline
                anomaly_counts = anomalies.groupby([anomalies['timestamp'].dt.date, 'anomaly_type']).size().reset_index(
                    name='count')
                fig = px.bar(anomaly_counts, x='timestamp', y='count', color='anomaly_type',
                             title="Anomalies Over Time",
                             labels={'count': 'Number of Anomalies', 'timestamp': 'Date'})
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No anomalies detected in the current data.")

    def export_data(self, filename_prefix: str = "waste_management"):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        bins_file = f"{filename_prefix}_bins_{timestamp}.csv"
        self.bins_df.to_csv(bins_file, index=False)

        if not self.sensor_data_df.empty:
            sensor_file = f"{filename_prefix}_sensor_data_{timestamp}.csv"
            self.sensor_data_df.to_csv(sensor_file, index=False)

        if not self.citizen_reports_df.empty:
            reports_file = f"{filename_prefix}_citizen_reports_{timestamp}.csv"
            self.citizen_reports_df.to_csv(reports_file, index=False)

        if not self.collection_routes_df.empty:
            routes_file = f"{filename_prefix}_collection_routes_{timestamp}.csv"
            self.collection_routes_df.to_csv(routes_file, index=False)

        return bins_file


def main():
    st.title("🗑️ IoT Waste Management System")
    st.markdown("---")

    if 'waste_system' not in st.session_state:
        st.session_state.waste_system = None
        st.session_state.data_generated = False

    st.sidebar.header("Configuration")
    num_bins = st.sidebar.slider("Number of Bins", 10, 100, 20)
    simulation_days = st.sidebar.slider("Simulation Days", 1, 30, 7)
    sensor_hours = st.sidebar.slider("Sensor Data Hours", 12, 168, 48)
    num_reports = st.sidebar.slider("Number of Citizen Reports", 5, 50, 15)

    if st.sidebar.button("Initialize System"):
        with st.spinner("Initializing system..."):
            st.session_state.waste_system = IoTWasteManagementSystem(
                num_bins=num_bins,
                simulation_days=simulation_days
            )
            st.session_state.data_generated = False
            st.success(f"Initialized system with {num_bins} bins!")

    if st.session_state.waste_system:
        if st.sidebar.button("Generate Data"):
            with st.spinner("Generating sensor data..."):
                sensor_data = st.session_state.waste_system.generate_sensor_data(hours=sensor_hours)

            with st.spinner("Generating citizen reports..."):
                citizen_reports = st.session_state.waste_system.generate_citizen_reports(num_reports=num_reports)

            st.session_state.data_generated = True
            st.success("Data generation complete!")

        if st.session_state.data_generated:
            st.header("System Performance")
            performance = st.session_state.waste_system.analyze_bin_performance()

            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Total Bins", performance['total_bins'])
            col2.metric("Bins Needing Collection", performance['bins_needing_collection'])
            col3.metric("Critical Bins", performance['critical_bins'])
            col4.metric("Average Fill Level", f"{performance['average_fill_level']:.1f}%")

            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Average Temperature", f"{performance['average_temperature']:.1f}°C")
            col2.metric("High Temp Alerts", performance['bins_with_high_temp'])
            col3.metric("Low Battery Warnings", performance['low_battery_bins'])
            col4.metric("Offline Devices", performance['offline_bins'])

            st.markdown("---")
            st.session_state.waste_system.visualize_data()

            st.markdown("---")
            st.header("Collection Route Optimization")
            if st.button("Optimize Collection Routes"):
                routes = st.session_state.waste_system.optimize_collection_routes()
                if not routes.empty:
                    st.dataframe(routes)

                    route_summary = st.session_state.waste_system.generate_reports().get('route_optimization', {})
                    if route_summary:
                        col1, col2, col3, col4 = st.columns(4)
                        col1.metric("Bins in Route", route_summary.get('bins_in_route', 0))
                        col2.metric("Total Estimated Time", route_summary.get('total_estimated_time', '0 minutes'))
                        col3.metric("Total Distance", route_summary.get('total_distance', '0 km'))
                        col4.metric("High Priority Bins", route_summary.get('high_priority_bins', 0))
                else:
                    st.info("No bins currently require collection.")

            st.markdown("---")
            st.header("Citizen Reports")
            if not st.session_state.waste_system.citizen_reports_df.empty:
                reports_summary = st.session_state.waste_system.generate_reports().get('citizen_reports', {})

                col1, col2, col3 = st.columns(3)
                col1.metric("Total Reports", reports_summary.get('total_reports', 0))
                col2.metric("Pending Reports", reports_summary.get('pending_reports', 0))
                col3.metric("Avg Resolution Time", reports_summary.get('average_resolution_time', '0 hours'))

                st.dataframe(st.session_state.waste_system.citizen_reports_df)
            else:
                st.info("No citizen reports available.")

            st.markdown("---")
            st.header("Data Export")
            if st.button("Export Data to CSV"):
                with st.spinner("Exporting data..."):
                    st.session_state.waste_system.export_data()
                st.success("Data exported successfully!")

        else:
            st.info("Please generate data to view the dashboard.")
    else:
        st.info("Please initialize the system using the sidebar controls.")


if __name__ == "__main__":
    main()