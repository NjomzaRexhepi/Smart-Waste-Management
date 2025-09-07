import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
from typing import Dict, List, Optional
import warnings
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import uuid

warnings.filterwarnings('ignore')

st.set_page_config(
    page_title="IoT Waste Management System Dashboard",
    page_icon="🗑️",
    layout="wide",
    initial_sidebar_state="expanded"
)

class CassandraConnection:
    def __init__(self, hosts=['127.0.0.1'], port=9042, keyspace='wastebin', 
                 username=None, password=None):
        self.hosts = hosts
        self.port = port
        self.keyspace = keyspace
        self.session = None
        self.cluster = None
        
        try:
            if username and password:
                auth_provider = PlainTextAuthProvider(username=username, password=password)
                self.cluster = Cluster(hosts, port=port, auth_provider=auth_provider)
            else:
                self.cluster = Cluster(hosts, port=port)
            
            self.session = self.cluster.connect()
            self.session.execute(f"USE {keyspace}")
            st.success("✅ Connected to Cassandra successfully!")
        except Exception as e:
            st.error(f"❌ Failed to connect to Cassandra: {str(e)}")
            self.session = None
    
    def execute_query(self, query: str, params=None) -> Optional[List]:
        if not self.session:
            st.error("No Cassandra connection available")
            return None
        
        try:
            if params:
                result = self.session.execute(query, params)
            else:
                result = self.session.execute(query)
            return list(result)
        except Exception as e:
            st.error(f"Query execution failed: {str(e)}")
            return None
    
    def close(self):
        if self.session:
            self.session.shutdown()
        if self.cluster:
            self.cluster.shutdown()

class CassandraWasteManagementSystem:
    def __init__(self, cassandra_conn: CassandraConnection):
        self.conn = cassandra_conn
        
    def get_bins_data(self, status_filter: str = None, bin_type_filter: str = None) -> pd.DataFrame:
        """Fetch bins data from Cassandra with optional filters"""
        query = "SELECT * FROM bins"
        conditions = []
        
        if status_filter and status_filter != "All":
            conditions.append(f"status = '{status_filter}'")
        
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        
        query += " ALLOW FILTERING"
        
        result = self.conn.execute_query(query)
        if result:
            df = pd.DataFrame(result)
            if bin_type_filter and bin_type_filter != "All" and 'type' in df.columns:
                df = df[df['type'] == bin_type_filter]
            return df
        return pd.DataFrame()
    
    def get_sensor_data(self, hours_back: int = 24, bin_ids: List[str] = None) -> pd.DataFrame:
        """Fetch recent sensor data from Cassandra"""
        cutoff_time = datetime.now() - timedelta(hours=hours_back)
        
        if bin_ids:
            all_data = []
            for bin_id in bin_ids:
                query = """
                SELECT * FROM sensor_data 
                WHERE bin_id = ? AND timestamp >= ?
                """
                result = self.conn.execute_query(query, [uuid.UUID(bin_id), cutoff_time])
                if result:
                    all_data.extend(result)
            
            if all_data:
                return pd.DataFrame(all_data)
        else:
            query = """
            SELECT * FROM sensor_data 
            WHERE timestamp >= ? ALLOW FILTERING
            """
            result = self.conn.execute_query(query, [cutoff_time])
            if result:
                return pd.DataFrame(result)
        
        return pd.DataFrame()
    
    def get_citizen_reports(self, days_back: int = 7, resolved_only: bool = None) -> pd.DataFrame:
        """Fetch citizen reports from Cassandra"""
        cutoff_time = datetime.now() - timedelta(days=days_back)
        
        query = """
        SELECT * FROM citizen_reports 
        WHERE report_timestamp >= ? ALLOW FILTERING
        """
        
        result = self.conn.execute_query(query, [cutoff_time])
        if result:
            df = pd.DataFrame(result)
            if resolved_only is not None:
                df = df[df['resolved'] == resolved_only]
            return df
        return pd.DataFrame()
    
    def get_maintenance_events(self, days_back: int = 30) -> pd.DataFrame:
        """Fetch maintenance events from Cassandra"""
        cutoff_time = datetime.now() - timedelta(days=days_back)
        
        query = """
        SELECT * FROM maintenance_events 
        WHERE event_timestamp >= ? ALLOW FILTERING
        """
        
        result = self.conn.execute_query(query, [cutoff_time])
        if result:
            return pd.DataFrame(result)
        return pd.DataFrame()
    
    def get_alarms(self, days_back: int = 7, severity: str = None, status: str = None) -> pd.DataFrame:
        """Fetch alarms from Cassandra"""
        cutoff_time = datetime.now() - timedelta(days=days_back)
        
        query = "SELECT * FROM alarms WHERE triggered_at >= ?"
        params = [cutoff_time]
        
        if severity:
            query += " AND severity = ?"
            params.append(severity)
        
        if status:
            query += " AND status = ?"
            params.append(status)
            
        query += " ALLOW FILTERING"
        
        result = self.conn.execute_query(query, params)
        if result:
            return pd.DataFrame(result)
        return pd.DataFrame()
    
    def get_route_history(self, days_back: int = 7) -> pd.DataFrame:
        """Fetch route history from Cassandra"""
        cutoff_time = datetime.now() - timedelta(days=days_back)
        
        query = """
        SELECT * FROM route_history 
        WHERE service_date >= ? ALLOW FILTERING
        """
        
        result = self.conn.execute_query(query, [cutoff_time])
        if result:
            return pd.DataFrame(result)
        return pd.DataFrame()
    
    def get_performance_analytics(self, days_back: int = 30) -> pd.DataFrame:
        """Fetch performance analytics from Cassandra"""
        cutoff_date = (datetime.now() - timedelta(days=days_back)).date()
        
        query = """
        SELECT * FROM performance_analytics 
        WHERE date >= ? ALLOW FILTERING
        """
        
        result = self.conn.execute_query(query, [cutoff_date])
        if result:
            return pd.DataFrame(result)
        return pd.DataFrame()
    
    def get_fill_level_trends(self, bin_ids: List[str] = None, hours_back: int = 48) -> pd.DataFrame:
        """Get fill level trends for specific bins"""
        sensor_data = self.get_sensor_data(hours_back=hours_back, bin_ids=bin_ids)
        if not sensor_data.empty and 'fill_level' in sensor_data.columns:
            return sensor_data[['bin_id', 'timestamp', 'fill_level']].sort_values('timestamp')
        return pd.DataFrame()
    
    def get_bins_needing_attention(self) -> Dict:
        """Identify bins that need immediate attention"""
        bins_df = self.get_bins_data()
        if bins_df.empty:
            return {}
        
        attention_data = {
            'high_fill_bins': [],
            'temperature_alerts': [],
            'offline_bins': [],
            'maintenance_due': []
        }
        
        # Check current fill levels
        for _, bin_row in bins_df.iterrows():
            if bin_row['current_fill_level'] >= 85:
                attention_data['high_fill_bins'].append({
                    'bin_id': str(bin_row['bin_id']),
                    'fill_level': bin_row['current_fill_level'],
                    'location': f"({bin_row['location_lat']}, {bin_row['location_lng']})"
                })
            
            if bin_row['status'] != 'active':
                attention_data['offline_bins'].append({
                    'bin_id': str(bin_row['bin_id']),
                    'status': bin_row['status']
                })
        
        # Check for maintenance due
        thirty_days_ago = datetime.now() - timedelta(days=30)
        for _, bin_row in bins_df.iterrows():
            if bin_row['last_maintenance_date'] and bin_row['last_maintenance_date'] < thirty_days_ago:
                attention_data['maintenance_due'].append({
                    'bin_id': str(bin_row['bin_id']),
                    'last_maintenance': bin_row['last_maintenance_date'].strftime('%Y-%m-%d')
                })
        
        return attention_data

def create_dashboard_visualizations(waste_system: CassandraWasteManagementSystem):
    """Create comprehensive dashboard visualizations"""
    
    # Sidebar filters
    st.sidebar.header("🔍 Data Filters")
    
    # Time filters
    time_range = st.sidebar.selectbox(
        "Time Range for Sensor Data",
        ["Last 6 hours", "Last 24 hours", "Last 48 hours", "Last Week"],
        index=1
    )
    
    time_mapping = {
        "Last 6 hours": 6,
        "Last 24 hours": 24,
        "Last 48 hours": 48,
        "Last Week": 168
    }
    
    hours_back = time_mapping[time_range]
    
    # Bin filters
    bins_df = waste_system.get_bins_data()
    if not bins_df.empty:
        bin_types = ["All"] + list(bins_df['type'].unique()) if 'type' in bins_df.columns else ["All"]
        bin_statuses = ["All"] + list(bins_df['status'].unique()) if 'status' in bins_df.columns else ["All"]
    else:
        bin_types = ["All"]
        bin_statuses = ["All"]
    
    selected_type = st.sidebar.selectbox("Bin Type Filter", bin_types)
    selected_status = st.sidebar.selectbox("Bin Status Filter", bin_statuses)
    
    # Main dashboard tabs
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "📊 Overview", "🗑️ Bin Status", "📈 Sensor Analytics", 
        "🚨 Alerts & Reports", "🚛 Routes & Maintenance", "📋 Performance"
    ])
    
    with tab1:
        st.header("System Overview")
        
        bins_df = waste_system.get_bins_data(status_filter=selected_status, bin_type_filter=selected_type)
        sensor_data = waste_system.get_sensor_data(hours_back=hours_back)
        reports_df = waste_system.get_citizen_reports()
        alarms_df = waste_system.get_alarms()
        
        col1, col2, col3, col4, col5 = st.columns(5)
        
        with col1:
            total_bins = len(bins_df) if not bins_df.empty else 0
            st.metric("Total Bins", total_bins)
        
        with col2:
            if not bins_df.empty and 'current_fill_level' in bins_df.columns:
                high_fill = len(bins_df[bins_df['current_fill_level'] >= 80])
                st.metric("Bins >80% Full", high_fill)
            else:
                st.metric("Bins >80% Full", "N/A")
        
        with col3:
            if not reports_df.empty:
                pending_reports = len(reports_df[reports_df['resolved'] == False])
                st.metric("Pending Reports", pending_reports)
            else:
                st.metric("Pending Reports", 0)
        
        with col4:
            if not alarms_df.empty:
                active_alarms = len(alarms_df[alarms_df['status'] != 'resolved'])
                st.metric("Active Alarms", active_alarms)
            else:
                st.metric("Active Alarms", 0)
        
        with col5:
            if not bins_df.empty and 'current_fill_level' in bins_df.columns:
                avg_fill = bins_df['current_fill_level'].mean()
                st.metric("Avg Fill Level", f"{avg_fill:.1f}%")
            else:
                st.metric("Avg Fill Level", "N/A")
        
        st.subheader("🚨 Bins Needing Attention")
        attention_data = waste_system.get_bins_needing_attention()
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.write("**High Fill Level (≥85%)**")
            if attention_data.get('high_fill_bins'):
                for bin_info in attention_data['high_fill_bins'][:5]:
                    st.write(f"• {bin_info['bin_id']}: {bin_info['fill_level']:.1f}%")
            else:
                st.write("No bins with high fill levels")
        
        with col2:
            st.write("**Offline Bins**")
            if attention_data.get('offline_bins'):
                for bin_info in attention_data['offline_bins'][:5]:
                    st.write(f"• {bin_info['bin_id']}: {bin_info['status']}")
            else:
                st.write("All bins online")
        
        with col3:
            st.write("**Maintenance Due**")
            if attention_data.get('maintenance_due'):
                for bin_info in attention_data['maintenance_due'][:5]:
                    st.write(f"• {bin_info['bin_id']}: {bin_info['last_maintenance']}")
            else:
                st.write("No maintenance overdue")
    
    with tab2:
        st.header("Bin Status Analysis")
        
        bins_df = waste_system.get_bins_data(status_filter=selected_status, bin_type_filter=selected_type)
        
        if not bins_df.empty:
            col1, col2 = st.columns(2)
            
            with col1:
                if 'current_fill_level' in bins_df.columns:
                    fig = px.histogram(
                        bins_df, 
                        x='current_fill_level',
                        title="Fill Level Distribution",
                        labels={'current_fill_level': 'Fill Level (%)'},
                        nbins=20
                    )
                    fig.add_vline(x=80, line_dash="dash", line_color="red", annotation_text="Collection Threshold")
                    st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                if 'status' in bins_df.columns:
                    status_counts = bins_df['status'].value_counts()
                    fig = px.pie(
                        values=status_counts.values,
                        names=status_counts.index,
                        title="Bin Status Distribution"
                    )
                    st.plotly_chart(fig, use_container_width=True)
            
            # Bin locations map
            if 'location_lat' in bins_df.columns and 'location_lng' in bins_df.columns:
                st.subheader("Bin Locations")
                
                # Create color coding based on fill level
                if 'current_fill_level' in bins_df.columns:
                    bins_df['color'] = bins_df['current_fill_level'].apply(
                        lambda x: 'red' if x >= 85 else 'orange' if x >= 70 else 'green'
                    )
                    
                    fig = px.scatter_mapbox(
                        bins_df,
                        lat='location_lat',
                        lon='location_lng',
                        color='current_fill_level',
                        hover_data=['bin_id', 'type', 'status'],
                        color_continuous_scale='RdYlGn_r',
                        title="Bin Locations by Fill Level",
                        zoom=12
                    )
                    fig.update_layout(mapbox_style="open-street-map")
                    st.plotly_chart(fig, use_container_width=True)
            
            # Data table
            st.subheader("Bin Details")
            display_columns = ['bin_id', 'type', 'current_fill_level', 'status', 'last_maintenance_date']
            available_columns = [col for col in display_columns if col in bins_df.columns]
            st.dataframe(bins_df[available_columns])
        else:
            st.warning("No bin data available with current filters")
    
    with tab3:
        st.header("Sensor Data Analytics")
        
        sensor_data = waste_system.get_sensor_data(hours_back=hours_back)
        
        if not sensor_data.empty:
            # Time series analysis
            st.subheader("Fill Level Trends")
            
            # Select bins for trend analysis
            unique_bins = sensor_data['bin_id'].unique()
            selected_bins = st.multiselect(
                "Select bins for trend analysis:",
                options=[str(bin_id) for bin_id in unique_bins],
                default=[str(unique_bins[0])] if len(unique_bins) > 0 else []
            )
            
            if selected_bins:
                trend_data = sensor_data[sensor_data['bin_id'].isin([uuid.UUID(bid) for bid in selected_bins])]
                
                if not trend_data.empty and 'fill_level' in trend_data.columns:
                    fig = px.line(
                        trend_data,
                        x='timestamp',
                        y='fill_level',
                        color='bin_id',
                        title="Fill Level Trends Over Time",
                        labels={'fill_level': 'Fill Level (%)', 'timestamp': 'Time'}
                    )
                    st.plotly_chart(fig, use_container_width=True)
            
            # Sensor metrics analysis
            col1, col2 = st.columns(2)
            
            with col1:
                if 'temperature' in sensor_data.columns:
                    fig = px.histogram(
                        sensor_data,
                        x='temperature',
                        title="Temperature Distribution",
                        labels={'temperature': 'Temperature (°C)'},
                        nbins=20
                    )
                    st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                if 'weight' in sensor_data.columns:
                    fig = px.box(
                        sensor_data,
                        y='weight',
                        title="Weight Distribution",
                        labels={'weight': 'Weight (kg)'}
                    )
                    st.plotly_chart(fig, use_container_width=True)
            
            # Correlation analysis
            numeric_columns = sensor_data.select_dtypes(include=[np.number]).columns
            if len(numeric_columns) > 1:
                st.subheader("Sensor Correlation Matrix")
                corr_matrix = sensor_data[numeric_columns].corr()
                
                fig = px.imshow(
                    corr_matrix,
                    title="Sensor Data Correlation Matrix",
                    color_continuous_scale='RdBu'
                )
                st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("No sensor data available for the selected time range")
    
    with tab4:
        st.header("Alerts & Citizen Reports")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("🚨 System Alarms")
            alarms_df = waste_system.get_alarms(days_back=7)
            
            if not alarms_df.empty:
                # Alarm severity distribution
                severity_counts = alarms_df['severity'].value_counts()
                fig = px.bar(
                    x=severity_counts.index,
                    y=severity_counts.values,
                    title="Alarms by Severity",
                    labels={'x': 'Severity', 'y': 'Count'}
                )
                st.plotly_chart(fig, use_container_width=True)
                
                # Recent alarms table
                st.write("**Recent Alarms:**")
                display_alarms = alarms_df[['alarm_type', 'severity', 'status', 'triggered_at']].head(10)
                st.dataframe(display_alarms)
            else:
                st.info("No alarms in the selected period")
        
        with col2:
            st.subheader("📢 Citizen Reports")
            reports_df = waste_system.get_citizen_reports(days_back=7)
            
            if not reports_df.empty:
                # Report type distribution
                type_counts = reports_df['report_type'].value_counts()
                fig = px.pie(
                    values=type_counts.values,
                    names=type_counts.index,
                    title="Reports by Type"
                )
                st.plotly_chart(fig, use_container_width=True)
                
                # Resolution status
                resolved_count = len(reports_df[reports_df['resolved'] == True])
                total_count = len(reports_df)
                resolution_rate = (resolved_count / total_count * 100) if total_count > 0 else 0
                
                st.metric("Resolution Rate", f"{resolution_rate:.1f}%")
                
                # Recent reports table
                st.write("**Recent Reports:**")
                display_reports = reports_df[['report_type', 'resolved', 'report_timestamp']].head(10)
                st.dataframe(display_reports)
            else:
                st.info("No citizen reports in the selected period")
    
    with tab5:
        st.header("Routes & Maintenance")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("🚛 Collection Routes")
            routes_df = waste_system.get_route_history(days_back=7)
            
            if not routes_df.empty:
                # Collection status distribution
                status_counts = routes_df['collection_status'].value_counts()
                fig = px.bar(
                    x=status_counts.index,
                    y=status_counts.values,
                    title="Collection Status Distribution",
                    labels={'x': 'Status', 'y': 'Count'}
                )
                st.plotly_chart(fig, use_container_width=True)
                
                # Daily collection trends
                routes_df['service_date'] = pd.to_datetime(routes_df['service_date']).dt.date
                daily_collections = routes_df.groupby('service_date').size().reset_index(name='collections')
                
                fig = px.line(
                    daily_collections,
                    x='service_date',
                    y='collections',
                    title="Daily Collections Trend"
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No route history available")
        
        with col2:
            st.subheader("🔧 Maintenance Events")
            maintenance_df = waste_system.get_maintenance_events(days_back=30)
            
            if not maintenance_df.empty:
                # Maintenance type distribution
                type_counts = maintenance_df['maintenance_type'].value_counts()
                fig = px.pie(
                    values=type_counts.values,
                    names=type_counts.index,
                    title="Maintenance Types"
                )
                st.plotly_chart(fig, use_container_width=True)
                
                # Cost analysis
                if 'cost' in maintenance_df.columns:
                    total_cost = maintenance_df['cost'].sum()
                    avg_cost = maintenance_df['cost'].mean()
                    
                    col2_1, col2_2 = st.columns(2)
                    col2_1.metric("Total Maintenance Cost", f"${total_cost:.2f}")
                    col2_2.metric("Average Cost per Event", f"${avg_cost:.2f}")
                
                # Recent maintenance table
                st.write("**Recent Maintenance:**")
                display_maintenance = maintenance_df[['maintenance_type', 'performed_by', 'duration', 'cost']].head(10)
                st.dataframe(display_maintenance)
            else:
                st.info("No maintenance events in the selected period")
    
    with tab6:
        st.header("Performance Analytics")
        
        performance_df = waste_system.get_performance_analytics(days_back=30)
        
        if not performance_df.empty:
            # Performance trends
            fig = make_subplots(
                rows=2, cols=2,
                subplot_titles=('Average Fill Level', 'Reports Trend', 'Response Time', 'Alarm Count'),
                specs=[[{"secondary_y": False}, {"secondary_y": False}],
                       [{"secondary_y": False}, {"secondary_y": False}]]
            )
            
            # Average fill level trend
            fig.add_trace(
                go.Scatter(x=performance_df['date'], y=performance_df['avg_fill_level'], 
                          name='Avg Fill Level'),
                row=1, col=1
            )
            
            # Reports trend
            fig.add_trace(
                go.Scatter(x=performance_df['date'], y=performance_df['total_reports'],
                          name='Total Reports', line=dict(color='orange')),
                row=1, col=2
            )
            
            # Response time
            fig.add_trace(
                go.Scatter(x=performance_df['date'], y=performance_df['avg_response_time'],
                          name='Avg Response Time', line=dict(color='green')),
                row=2, col=1
            )
            
            # Alarm count
            fig.add_trace(
                go.Scatter(x=performance_df['date'], y=performance_df['alarms_count'],
                          name='Alarms Count', line=dict(color='red')),
                row=2, col=2
            )
            
            fig.update_layout(height=600, showlegend=False, title_text="Performance Trends")
            st.plotly_chart(fig, use_container_width=True)
            
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                avg_fill = performance_df['avg_fill_level'].mean()
                st.metric("Avg Fill Level", f"{avg_fill:.1f}%")
            
            with col2:
                total_reports = performance_df['total_reports'].sum()
                st.metric("Total Reports", total_reports)
            
            with col3:
                avg_response = performance_df['avg_response_time'].mean()
                st.metric("Avg Response Time", f"{avg_response:.1f}h")
            
            with col4:
                total_alarms = performance_df['alarms_count'].sum()
                st.metric("Total Alarms", total_alarms)
        else:
            st.warning("No performance analytics data available")

def main():
    st.title("🗑️ IoT Waste Management System - Cassandra Edition")
    st.markdown("---")
    
    st.sidebar.header("🔧 Cassandra Configuration")
    
    cassandra_host = st.sidebar.text_input("Cassandra Host", value="127.0.0.1")
    cassandra_port = st.sidebar.number_input("Port", value=9042, min_value=1, max_value=65535)
    keyspace = st.sidebar.text_input("Keyspace", value="wastebin")
    
    use_auth = st.sidebar.checkbox("Use Authentication")
    username = st.sidebar.text_input("Username", value="") if use_auth else None
    password = st.sidebar.text_input("Password", type="password", value="") if use_auth else None
    
    if st.sidebar.button("Connect to Cassandra"):
        st.session_state.cassandra_conn = CassandraConnection(
            hosts=[cassandra_host],
            port=cassandra_port,
            keyspace=keyspace,
            username=username,
            password=password
        )
        
        if st.session_state.cassandra_conn.session:
            st.session_state.waste_system = CassandraWasteManagementSystem(st.session_state.cassandra_conn)
    
    if hasattr(st.session_state, 'waste_system') and st.session_state.waste_system:
        create_dashboard_visualizations(st.session_state.waste_system)
        
        if st.sidebar.button("🔄 Refresh Data"):
            st.rerun()
        
        st.sidebar.markdown("---")
        st.sidebar.header("📤 Data Export")
        
        if st.sidebar.button("Export Current Data"):
            with st.spinner("Exporting data..."):
                bins_data = st.session_state.waste_system.get_bins_data()
                sensor_data = st.session_state.waste_system.get_sensor_data(hours_back=24)
                
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                
                if not bins_data.empty:
                    bins_data.to_csv(f"bins_export_{timestamp}.csv", index=False)
                
                if not sensor_data.empty:
                    sensor_data.to_csv(f"sensor_export_{timestamp}.csv", index=False)
                
                st.sidebar.success("Data exported successfully!")
    
    else:
        st.info("👈 Please configure and connect to your Cassandra database using the sidebar.")
        
        st.header("📋 Database Schema Overview")
        
        schema_info = {
            "bins": {
                "description": "Main bins information table",
                "columns": ["bin_id (UUID)", "location_lat (double)", "location_lng (double)", 
                          "type (text)", "capacity (double)", "current_fill_level (double)", 
                          "status (text)", "last_maintenance_date (timestamp)", "installation_date (timestamp)"]
            },
            "sensor_data": {
                "description": "Real-time sensor readings",
                "columns": ["sensor_id (UUID)", "bin_id (UUID)", "timestamp (timestamp)", 
                          "fill_level (double)", "temperature (double)", "weight (double)", 
                          "humidity (double)", "status (text)"]
            },
            "citizen_reports": {
                "description": "Citizen-submitted reports",
                "columns": ["report_id (UUID)", "bin_id (UUID)", "report_timestamp (timestamp)", 
                          "report_type (text)", "description (text)", "resolved (boolean)", 
                          "resolved_timestamp (timestamp)"]
            },
            "maintenance_events": {
                "description": "Maintenance activity logs",
                "columns": ["event_id (UUID)", "bin_id (UUID)", "maintenance_type (text)", 
                          "event_timestamp (timestamp)", "performed_by (text)", 
                          "duration (double)", "cost (double)"]
            },
            "alarms": {
                "description": "System alerts and alarms",
                "columns": ["alarm_id (UUID)", "bin_id (UUID)", "alarm_type (text)", 
                          "triggered_at (timestamp)", "resolved_at (timestamp)", 
                          "severity (text)", "status (text)"]
            },
            "route_history": {
                "description": "Collection route tracking",
                "columns": ["route_id (UUID)", "bin_id (UUID)", "service_date (timestamp)", 
                          "collection_status (text)"]
            },
            "performance_analytics": {
                "description": "Daily performance metrics",
                "columns": ["id (UUID)", "date (date)", "total_bins (int)", 
                          "avg_fill_level (double)", "total_reports (int)", 
                          "resolved_reports (int)", "maintenance_events_count (int)", 
                          "avg_response_time (double)", "alarms_count (int)", "routes_completed (int)"]
            }
        }
        
        for table_name, info in schema_info.items():
            with st.expander(f"📋 {table_name.upper()} Table"):
                st.write(f"**Description:** {info['description']}")
                st.write("**Columns:**")
                for col in info['columns']:
                    st.write(f"• {col}")
        
        st.markdown("---")
        st.header("🎯 Available Dashboard Features")
        
        feature_categories = {
            "📊 Overview Dashboard": [
                "Real-time system metrics and KPIs",
                "Bins requiring immediate attention",
                "System health status overview",
                "Key performance indicators"
            ],
            "🗑️ Bin Management": [
                "Interactive bin location mapping",
                "Fill level distribution analysis",
                "Bin status monitoring",
                "Capacity utilization tracking",
                "Filter by type, status, and location"
            ],
            "📈 Sensor Analytics": [
                "Time-series analysis of sensor data",
                "Fill level trend monitoring",
                "Temperature and weight analysis",
                "Sensor correlation matrices",
                "Multi-bin comparison views"
            ],
            "🚨 Alert Management": [
                "System alarm monitoring and categorization",
                "Citizen report tracking and analysis",
                "Alert severity distribution",
                "Resolution rate monitoring",
                "Report type analytics"
            ],
            "🚛 Operations Management": [
                "Collection route optimization",
                "Maintenance scheduling and tracking",
                "Route efficiency analysis",
                "Maintenance cost analysis",
                "Service completion tracking"
            ],
            "📋 Performance Analytics": [
                "Historical performance trends",
                "System efficiency metrics",
                "Response time analysis",
                "Comparative performance charts",
                "Monthly/weekly summaries"
            ]
        }
        
        for category, features in feature_categories.items():
            with st.expander(category):
                for feature in features:
                    st.write(f"✅ {feature}")
        
        st.markdown("---")
        st.header("🔍 Advanced Filtering Options")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Time-based Filters")
            st.write("• Last 6 hours to 1 week for sensor data")
            st.write("• Last 7-30 days for reports and maintenance")
            st.write("• Custom date ranges for performance analytics")
            st.write("• Real-time data refresh capabilities")
            
        with col2:
            st.subheader("Categorical Filters")
            st.write("• Filter by bin type (street, residential, commercial)")
            st.write("• Filter by bin status (active, maintenance, offline)")
            st.write("• Filter by district or geographic area")
            st.write("• Filter by alarm severity and status")
        
        st.markdown("---")
        st.header("📊 Sample Visualizations Available")
        
        viz_types = {
            "Maps & Location": [
                "Interactive scatter maps with fill level color coding",
                "Heatmaps for bin density and performance",
                "Route visualization on maps"
            ],
            "Time Series": [
                "Fill level trends over time",
                "Temperature monitoring charts",
                "Performance metric trends",
                "Collection frequency analysis"
            ],
            "Statistical Charts": [
                "Fill level distribution histograms",
                "Box plots for sensor data analysis",
                "Correlation matrices for sensor relationships",
                "Bar charts for categorical data"
            ],
            "Comparative Analysis": [
                "Multi-bin performance comparisons",
                "District-wise analysis charts",
                "Before/after maintenance comparisons",
                "Seasonal trend analysis"
            ]
        }
        
        for viz_category, viz_list in viz_types.items():
            with st.expander(f"📈 {viz_category}"):
                for viz in viz_list:
                    st.write(f"📊 {viz}")

def setup_sample_data_generation():
    """Helper function to generate sample data for testing"""
    st.sidebar.markdown("---")
    st.sidebar.header("🧪 Sample Data")
    
    if st.sidebar.button("Generate Sample CQL Queries"):
        st.header("📝 Sample CQL Queries for Testing")
        
        sample_queries = {
            "Insert Sample Bin": """
            INSERT INTO bins (bin_id, location_lat, location_lng, type, capacity, 
                            current_fill_level, status, last_maintenance_date, installation_date)
            VALUES (uuid(), 42.6629, 21.1655, 'street', 120.0, 45.5, 'active', 
                   toTimestamp(now()), toTimestamp(now()));
            """,
            
            "Insert Sample Sensor Data": """
            INSERT INTO sensor_data (sensor_id, bin_id, timestamp, fill_level, 
                                   temperature, weight, humidity, status)
            VALUES (uuid(), ?, toTimestamp(now()), 67.5, 23.4, 81.2, 65.0, 'OK');
            """,
            
            "Insert Sample Report": """
            INSERT INTO citizen_reports (report_id, bin_id, report_timestamp, 
                                       report_type, description, resolved, resolved_timestamp)
            VALUES (uuid(), ?, toTimestamp(now()), 'overflow', 
                   'Bin is overflowing with waste', false, null);
            """,
            
            "Query High Fill Bins": """
            SELECT bin_id, current_fill_level, location_lat, location_lng 
            FROM bins 
            WHERE current_fill_level >= 80.0 ALLOW FILTERING;
            """,
            
            "Query Recent Sensor Data": """
            SELECT * FROM sensor_data 
            WHERE timestamp >= '2024-01-01' ALLOW FILTERING
            LIMIT 100;
            """,
            
            "Query Unresolved Reports": """
            SELECT * FROM citizen_reports 
            WHERE resolved = false ALLOW FILTERING;
            """
        }
        
        for query_name, query in sample_queries.items():
            with st.expander(f"🔍 {query_name}"):
                st.code(query, language="sql")

def calculate_collection_priority(fill_level, temperature, last_collection_hours):
    """Calculate collection priority score"""
    base_score = fill_level
    temp_factor = max(0, (temperature - 20) * 2)  
    time_factor = min(last_collection_hours / 24 * 10, 20)  
    
    return min(100, base_score + temp_factor + time_factor)

def detect_sensor_anomalies(sensor_df):
    """Detect anomalies in sensor data using statistical methods"""
    anomalies = []
    
    if sensor_df.empty:
        return pd.DataFrame()
    
    for bin_id, bin_data in sensor_df.groupby('bin_id'):
        bin_data = bin_data.sort_values('timestamp')
        
        if 'temperature' in bin_data.columns:
            temp_mean = bin_data['temperature'].mean()
            temp_std = bin_data['temperature'].std()
            temp_upper = temp_mean + 2 * temp_std
            temp_lower = temp_mean - 2 * temp_std
            
            temp_anomalies = bin_data[
                (bin_data['temperature'] > temp_upper) | 
                (bin_data['temperature'] < temp_lower)
            ]
            
            for _, row in temp_anomalies.iterrows():
                anomalies.append({
                    'bin_id': str(bin_id),
                    'timestamp': row['timestamp'],
                    'anomaly_type': 'Temperature Anomaly',
                    'value': row['temperature'],
                    'expected_range': f"{temp_lower:.1f} - {temp_upper:.1f}",
                    'severity': 'High' if abs(row['temperature'] - temp_mean) > 3 * temp_std else 'Medium'
                })
        
        if 'fill_level' in bin_data.columns and len(bin_data) > 1:
            fill_diff = bin_data['fill_level'].diff().abs()
            sudden_changes = bin_data[fill_diff > 30]  
            
            for _, row in sudden_changes.iterrows():
                anomalies.append({
                    'bin_id': str(bin_id),
                    'timestamp': row['timestamp'],
                    'anomaly_type': 'Sudden Fill Level Change',
                    'value': row['fill_level'],
                    'expected_range': 'Gradual change expected',
                    'severity': 'Medium'
                })
    
    return pd.DataFrame(anomalies)

if __name__ == "__main__":
    if 'cassandra_conn' not in st.session_state:
        st.session_state.cassandra_conn = None
    if 'waste_system' not in st.session_state:
        st.session_state.waste_system = None
    
    main()
    
    setup_sample_data_generation()