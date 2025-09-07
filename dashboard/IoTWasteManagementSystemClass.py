import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import uuid
import warnings
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from typing import Dict, List, Optional

warnings.filterwarnings('ignore')

st.set_page_config(
    page_title="IoT Waste Management System with Cassandra",
    page_icon="🗑️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# -------------------- Cassandra Connection --------------------
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
            # Debug prints (remove in production)
            if params is not None:
                print(f"Executing query: {query}")
                print(f"Params: {params}")
                print(f"Number of placeholders (?): {query.count('?')}")
                print(f"Number of params: {len(params)}")
            
            if params is not None:
                # Ensure params is always a tuple
                if not isinstance(params, tuple):
                    params = tuple(params)
                # Use prepared statement to avoid potential binding issues
                prepared = self.session.prepare(query)
                result = self.session.execute(prepared, params)
            else:
                result = self.session.execute(query)
            return list(result)
        except Exception as e:
            st.error(f"Query execution failed: {str(e)}")
            # Also print for debugging
            print(f"Failed query: {query}")
            print(f"Failed params: {params}")
            return None

    def close(self):
        if self.session:
            self.session.shutdown()
        if self.cluster:
            self.cluster.shutdown()

# -------------------- Waste Management System --------------------
class CassandraWasteManagementSystem:
    def __init__(self, cassandra_conn: CassandraConnection):
        self.conn = cassandra_conn

    def get_bins_data(self, status_filter: str = None, bin_type_filter: str = None) -> pd.DataFrame:
        query = "SELECT * FROM bins"
        conditions = []
        params = []

        if status_filter and status_filter != "All":
            conditions.append("status = ?")
            params.append(status_filter)

        if bin_type_filter and bin_type_filter != "All":
            conditions.append("type = ?")
            params.append(bin_type_filter)

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        query += " ALLOW FILTERING"

        result = self.conn.execute_query(query, params if params else None)
        if result:
            df = pd.DataFrame(result)
            return df
        return pd.DataFrame()

    def get_sensor_data(self, hours_back: int = 24, bin_ids: List[str] = None) -> pd.DataFrame:
        cutoff_time = datetime.now() - timedelta(hours=hours_back)
        all_data = []

        if bin_ids:
            for bin_id in bin_ids:
                query = """
                SELECT * FROM sensor_data 
                WHERE bin_id = ? AND timestamp >= ?
                """
                result = self.conn.execute_query(query, (uuid.UUID(bin_id), cutoff_time))
                if result:
                    all_data.extend(result)
        else:
            query = "SELECT * FROM sensor_data WHERE timestamp >= ? ALLOW FILTERING"
            result = self.conn.execute_query(query, (cutoff_time,))
            if result:
                all_data.extend(result)

        if all_data:
            return pd.DataFrame(all_data)
        return pd.DataFrame()

    def get_citizen_reports(self, days_back: int = 7, resolved_only: bool = None) -> pd.DataFrame:
        cutoff_time = datetime.now() - timedelta(days=days_back)
        query = "SELECT * FROM citizen_reports WHERE report_timestamp >= ? ALLOW FILTERING"
        result = self.conn.execute_query(query, (cutoff_time,))
        if result:
            df = pd.DataFrame(result)
            if resolved_only is not None:
                df = df[df['resolved'] == resolved_only]
            return df
        return pd.DataFrame()

    def get_maintenance_events(self, days_back: int = 30) -> pd.DataFrame:
        cutoff_time = datetime.now() - timedelta(days=days_back)
        query = "SELECT * FROM maintenance_events WHERE event_timestamp >= ? ALLOW FILTERING"
        result = self.conn.execute_query(query, (cutoff_time,))
        if result:
            return pd.DataFrame(result)
        return pd.DataFrame()

    def get_alarms(self, days_back: int = 7, severity: str = None, status: str = None) -> pd.DataFrame:
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
        result = self.conn.execute_query(query, tuple(params))
        if result:
            return pd.DataFrame(result)
        return pd.DataFrame()

    def get_route_history(self, days_back: int = 7) -> pd.DataFrame:
        cutoff_time = datetime.now() - timedelta(days=days_back)
        query = "SELECT * FROM route_history WHERE service_date >= ? ALLOW FILTERING"
        result = self.conn.execute_query(query, (cutoff_time,))
        if result:
            return pd.DataFrame(result)
        return pd.DataFrame()

    def get_performance_analytics(self, days_back: int = 30) -> pd.DataFrame:
        cutoff_date = (datetime.now() - timedelta(days=days_back)).date()
        query = "SELECT * FROM performance_analytics WHERE date >= ? ALLOW FILTERING"
        result = self.conn.execute_query(query, (cutoff_date,))
        if result:
            return pd.DataFrame(result)
        return pd.DataFrame()

    def get_bins_needing_attention(self) -> Dict:
        bins_df = self.get_bins_data()
        if bins_df.empty:
            return {}
        attention_data = {'high_fill_bins': [], 'offline_bins': [], 'maintenance_due': []}

        thirty_days_ago = datetime.now() - timedelta(days=30)

        for _, row in bins_df.iterrows():
            if row['current_fill_level'] >= 85:
                attention_data['high_fill_bins'].append({
                    'bin_id': str(row['bin_id']),
                    'fill_level': row['current_fill_level']
                })
            if row['status'] != 'active':
                attention_data['offline_bins'].append({
                    'bin_id': str(row['bin_id']),
                    'status': row['status']
                })
            if row['last_maintenance_date'] and row['last_maintenance_date'] < thirty_days_ago:
                attention_data['maintenance_due'].append({
                    'bin_id': str(row['bin_id']),
                    'last_maintenance': row['last_maintenance_date'].strftime('%Y-%m-%d')
                })
        return attention_data

# -------------------- Streamlit Dashboard --------------------
def main():
    st.title("🗑️ IoT Waste Management System - Cassandra Edition")
    st.sidebar.header("🔧 Cassandra Configuration")

    cassandra_host = st.sidebar.text_input("Cassandra Host", value="127.0.0.1")
    cassandra_port = st.sidebar.number_input("Port", value=9042, min_value=1, max_value=65535)
    keyspace = st.sidebar.text_input("Keyspace", value="wastebin")

    use_auth = st.sidebar.checkbox("Use Authentication")
    username = st.sidebar.text_input("Username") if use_auth else None
    password = st.sidebar.text_input("Password", type="password") if use_auth else None

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
        system = st.session_state.waste_system

        # -------- Bins Overview --------
        st.subheader("📊 Bin Overview")
        status_filter = st.selectbox("Filter by Status", ["All", "active", "inactive", "offline"])
        bin_type_filter = st.selectbox("Filter by Type", ["All", "recycling", "organic", "general"])
        bins_df = system.get_bins_data(status_filter=status_filter, bin_type_filter=bin_type_filter)
        if not bins_df.empty:
            st.dataframe(bins_df)
            fig_status = px.pie(bins_df, names='status', title="Bin Status Distribution")
            st.plotly_chart(fig_status)
        else:
            st.info("No bin data available.")

        # -------- Sensor Data --------
        st.subheader("🌡️ Sensor Data (Last 24h)")
        sensor_df = system.get_sensor_data(hours_back=24)
        if not sensor_df.empty:
            st.dataframe(sensor_df)
            if 'current_fill_level' in sensor_df.columns and 'timestamp' in sensor_df.columns:
                fig_fill = px.line(sensor_df, x='timestamp', y='current_fill_level', title="Bin Fill Levels")
                st.plotly_chart(fig_fill)
        else:
            st.info("No sensor data available.")

        # -------- Citizen Reports --------
        st.subheader("📣 Citizen Reports")
        resolved_only = st.selectbox("Show Only Resolved Reports?", ["All", "Resolved", "Unresolved"])
        resolved_flag = None
        if resolved_only == "Resolved":
            resolved_flag = True
        elif resolved_only == "Unresolved":
            resolved_flag = False
        reports_df = system.get_citizen_reports(days_back=7, resolved_only=resolved_flag)
        if not reports_df.empty:
            st.dataframe(reports_df)
            resolved_count = reports_df['resolved'].sum()
            unresolved_count = len(reports_df) - resolved_count
            fig_reports = px.pie(names=['Resolved', 'Unresolved'], values=[resolved_count, unresolved_count],
                                 title="Reports Resolution Status")
            st.plotly_chart(fig_reports)
        else:
            st.info("No citizen reports available.")

        # -------- Maintenance Events --------
        st.subheader("🛠️ Maintenance Events (Last 30 Days)")
        maintenance_df = system.get_maintenance_events(days_back=30)
        if not maintenance_df.empty:
            st.dataframe(maintenance_df)
        else:
            st.info("No maintenance events found.")

        # -------- Alarms --------
        st.subheader("🚨 Alarms (Last 7 Days)")
        severity = st.selectbox("Filter by Severity", ["All", "low", "medium", "high"])
        status = st.selectbox("Filter by Status", ["All", "active", "resolved"])
        severity_filter = None if severity == "All" else severity
        status_filter_alarm = None if status == "All" else status
        alarms_df = system.get_alarms(days_back=7, severity=severity_filter, status=status_filter_alarm)
        if not alarms_df.empty:
            st.dataframe(alarms_df)
            if 'severity' in alarms_df.columns:
                fig_alarms = px.histogram(alarms_df, x='severity', title="Alarm Severity Distribution")
                st.plotly_chart(fig_alarms)
        else:
            st.info("No alarms triggered.")

        # -------- Route History --------
        st.subheader("🗺️ Route History (Last 7 Days)")
        routes_df = system.get_route_history(days_back=7)
        if not routes_df.empty:
            st.dataframe(routes_df)
        else:
            st.info("No route history available.")

        # -------- Performance Analytics --------
        st.subheader("📈 Performance Analytics (Last 30 Days)")
        perf_df = system.get_performance_analytics(days_back=30)
        if not perf_df.empty:
            st.dataframe(perf_df)
            if 'bins_emptied' in perf_df.columns and 'date' in perf_df.columns:
                fig_perf = px.line(perf_df, x='date', y='bins_emptied', title="Bins Emptied Over Last 30 Days")
                st.plotly_chart(fig_perf)
        else:
            st.info("No performance data available.")

        # -------- Bins Needing Attention --------
        st.subheader("⚠️ Bins Needing Attention")
        attention_data = system.get_bins_needing_attention()
        if attention_data:
            st.json(attention_data)
        else:
            st.info("No bins needing attention.")

    else:
        st.info("👈 Please connect to Cassandra to view data.")

if __name__ == "__main__":
    if 'cassandra_conn' not in st.session_state:
        st.session_state.cassandra_conn = None
    if 'waste_system' not in st.session_state:
        st.session_state.waste_system = None
    main()