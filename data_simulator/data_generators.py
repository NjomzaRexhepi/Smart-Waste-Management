from faker import Faker
import random
from datetime import datetime, date, timezone

fake = Faker()

def generate_bin_location():
    return {
        "street": fake.street_address(),
        "district": random.choice(["Center", "Industrial", "Residential", "Commercial"]),
        "coordinates": {
            "lat": 42.66 + random.uniform(-0.05, 0.05),  # Prishtina approx latitude
            "lon": 21.17 + random.uniform(-0.05, 0.05)
        }
    }

def generate_bin_data(num_bins=50):
    bins = []
    for i in range(num_bins):
        location = generate_bin_location()
        bins.append({
            "bin_id": f"bin-{i:03d}",
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
    return {
        "report_id": f"report-{datetime.now().strftime('%Y%m%d%H%M%S%f')}",
        "bin_id": bin_id,
        "timestamp": datetime.utcnow().isoformat(),
        "issue_type": random.choice(issues),
        "description": fake.sentence(),
        "reporter_id": f"citizen-{random.randint(1000, 9999)}",
        "status": "reported"
    }
