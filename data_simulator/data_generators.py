import uuid
from faker import Faker
import random
from datetime import datetime, date, timezone

fake = Faker()

def generate_bin_location():
    return {
        "street": fake.street_address(),
        "district": random.choice(["Center", "Industrial", "Residential", "Commercial"]),
        "coordinates": {
            "lat": 42.66 + random.uniform(-0.05, 0.05),  
            "lon": 21.17 + random.uniform(-0.05, 0.05)
        }
    }

def generate_bin_data(num_bins=50):
    bins = []
    for i in range(num_bins):
        location = generate_bin_location()
        
        installation_date = fake.date_between(start_date='-2y', end_date='today')
        last_maintenance = fake.date_between(start_date='-6m', end_date='today')
        
        bins.append({
            "bin_id": str(uuid.uuid4()),
            "type": random.choice(["street", "residential", "commercial", "overflow"]),
            "capacity_kg": random.choice([60, 120, 240, 500]),
            "installation_date": fake.date_time_between(start_date='-2y', end_date='now', tzinfo=timezone.utc),
            "last_maintenance": fake.date_time_between(start_date='-6m', end_date='now', tzinfo=timezone.utc),
            "location": location,
            "status": "active"
        })
    return bins

def generate_citizen_report(bin_id):
    issues = [
        "Overflowing bin", "Bad odor", "Recycling contamination",
        "Damaged bin", "Illegal dumping", "Missed collection"
    ]
    
    timestamp = datetime.now(timezone.utc)
    
    return {
        "report_id": f"report-{timestamp.strftime('%Y%m%d%H%M%S%f')}",
        "bin_id": bin_id if bin_id else str(uuid.uuid4()),  
        "timestamp": timestamp.strftime('%Y-%m-%d %H:%M:%S'),  
        "issue_type": random.choice(issues),
        "description": fake.sentence(),
        "reporter_id": f"citizen-{random.randint(1000, 9999)}",
        "status": "reported"
    }

def validate_data(data_dict):
    """Validate that no critical fields are empty strings"""
    critical_fields = ['timestamp', 'installation_date', 'last_maintenance']
    
    for field in critical_fields:
        if field in data_dict and (data_dict[field] == '' or data_dict[field] is None):
            if 'timestamp' in field:
                data_dict[field] = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
            elif 'date' in field:
                data_dict[field] = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    
    return data_dict

def generate_safe_bin_data(num_bins=50):
    bins = generate_bin_data(num_bins)
    return [validate_data(bin_data) for bin_data in bins]

def generate_safe_citizen_report(bin_id):
    report = generate_citizen_report(bin_id)
    return validate_data(report)

if __name__ == "__main__":
    bins = generate_safe_bin_data(5)
    reports = [generate_safe_citizen_report(bin['bin_id']) for bin in bins[:3]]
    
    print("Sample bins:")
    for bin_data in bins:
        print(f"  {bin_data['bin_id']}: {bin_data['timestamp'] if 'timestamp' in bin_data else 'N/A'}")
    
    print("\nSample reports:")
    for report in reports:
        print(f"  {report['report_id']}: {report['timestamp']}")