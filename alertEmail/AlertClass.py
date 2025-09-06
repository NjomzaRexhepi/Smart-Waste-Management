import smtplib
import requests
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from kafka import KafkaConsumer
import json
from datetime import datetime
import logging
import time

class AlertSystem:
    def __init__(self):
        self.email_config = {
            'smtp_server': 'smtp.gmail.com',
            'smtp_port': 587,
            'username': 'your-email@gmail.com',
            'password': 'your-app-password'
        }
        
        # Kafka consumer for alarms
        self.consumer = KafkaConsumer(
            'alarms',
            bootstrap_servers=['localhost:9092'],
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        
        # Alert thresholds and cooldown periods
        self.alert_cooldown = {}  # Track last alert time for each bin
        self.cooldown_period = 300  # 5 minutes cooldown
        
        # Setup logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
    
    def send_email_alert(self, alert_data):
        """Send email alert"""
        try:
            msg = MIMEMultipart()
            msg['From'] = self.email_config['username']
            msg['To'] = 'maintenance-team@company.com'
            msg['Subject'] = f"URGENT: {alert_data['alarm_type']} - {alert_data['bin_id']}"
            
            body = f"""
            Alert Details:
            - Bin ID: {alert_data['bin_id']}
            - Alert Type: {alert_data['alarm_type']}
            - Value: {alert_data['value']}
            - Timestamp: {alert_data['timestamp']}
            - Location: View on map: https://maps.google.com/?q={alert_data.get('lat', '')},{alert_data.get('lon', '')}
            
            Please take immediate action.
            """
            
            msg.attach(MIMEText(body, 'plain'))
            
            server = smtplib.SMTP(self.email_config['smtp_server'], self.email_config['smtp_port'])
            server.starttls()
            server.login(self.email_config['username'], self.email_config['password'])
            
            text = msg.as_string()
            server.sendmail(self.email_config['username'], 'maintenance-team@company.com', text)
            server.quit()
            
            self.logger.info(f"Email alert sent for {alert_data['bin_id']}")
            
        except Exception as e:
            self.logger.error(f"Failed to send email: {e}")
    
      
    def send_sms_alert(self, alert_data, phone_numbers):
        """Send SMS alert using Twilio (requires Twilio account)"""
        try:
            # This would require Twilio credentials
            from twilio.rest import Client
            
            account_sid = 'your_account_sid'
            auth_token = 'your_auth_token'
            client = Client(account_sid, auth_token)
            
            message_body = f"URGENT: {alert_data['alarm_type']} at Bin {alert_data['bin_id']}. Value: {alert_data['value']}. Time: {alert_data['timestamp']}"
            
            for phone in phone_numbers:
                message = client.messages.create(
                    body=message_body,
                    from_='+1234567890',  # Your Twilio phone number
                    to=phone
                )
                self.logger.info(f"SMS alert sent to {phone} for {alert_data['bin_id']}")
                
        except Exception as e:
            self.logger.error(f"Failed to send SMS alert: {e}")
    
    def check_alert_cooldown(self, bin_id):
        """Check if we should send alert based on cooldown period"""
        current_time = time.time()
        
        if bin_id in self.alert_cooldown:
            time_since_last = current_time - self.alert_cooldown[bin_id]
            if time_since_last < self.cooldown_period:
                return False
        
        self.alert_cooldown[bin_id] = current_time
        return True
    
    def process_alert(self, alert_data):
        """Process and route alerts based on severity"""
        bin_id = alert_data['bin_id']
        alarm_type = alert_data['alarm_type']
        
        # Check cooldown to prevent spam
        if not self.check_alert_cooldown(bin_id):
            self.logger.info(f"Alert for {bin_id} skipped due to cooldown")
            return
        
        self.logger.info(f"Processing alert for {bin_id}: {alarm_type}")
        
        # Route alerts based on type and severity
        if alarm_type == 'High Fill Level':
            # High priority - send all types of alerts
            self.send_email_alert(alert_data)
            self.send_slack_alert(alert_data)
            self.send_teams_alert(alert_data)
            
            # SMS for critical alerts
            emergency_contacts = ['+1234567890', '+0987654321']
            self.send_sms_alert(alert_data, emergency_contacts)
            
        elif alarm_type in ['Low Battery', 'Connectivity Issue']:
            # Medium priority - email and chat only
            self.send_email_alert(alert_data)
            self.send_slack_alert(alert_data)
            
        elif alarm_type == 'Sensor Malfunction':
            # High priority technical issue
            self.send_email_alert(alert_data)
            self.send_slack_alert(alert_data)
            
        else:
            # Default - send to Slack
            self.send_slack_alert(alert_data)
    
    def start_listening(self):
        """Start listening for alerts from Kafka"""
        self.logger.info("Starting alert system - listening for messages...")
        
        try:
            for message in self.consumer:
                alert_data = message.value
                self.logger.info(f"Received alert: {alert_data}")
                
                # Add timestamp if not present
                if 'timestamp' not in alert_data:
                    alert_data['timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
                # Process the alert
                self.process_alert(alert_data)
                
        except KeyboardInterrupt:
            self.logger.info("Alert system stopped by user")
        except Exception as e:
            self.logger.error(f"Error in alert system: {e}")
        finally:
            self.consumer.close()
    
    def send_test_alert(self):
        """Send a test alert to verify system is working"""
        test_alert = {
            'bin_id': 'TEST_BIN_001',
            'alarm_type': 'System Test',
            'value': 'Test Message',
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'lat': '40.7128',
            'lon': '-74.0060'
        }
        
        self.logger.info("Sending test alert...")
        self.process_alert(test_alert)


if __name__ == "__main__":
    # Initialize and start the alert system
    alert_system = AlertSystem()
    
    # Uncomment to send a test alert
    # alert_system.send_test_alert()
    
    # Start listening for real alerts
    alert_system.start_listening()