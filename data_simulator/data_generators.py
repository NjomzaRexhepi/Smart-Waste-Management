# from faker import Faker
# import random
# from datetime import datetime

# fake = Faker()

# def generate_bin_location():
#     return {
#         "street": fake.street_address(),
#         "district": random.choice(["Center", "Industrial", "Residential", "Commercial"]),
#         "coordinates": {
#             "lat": 42.66 + random.uniform(-0.05, 0.05),  # Prishtina approx latitude
#             "lon": 21.17 + random.uniform(-0.05, 0.05)
#         }
#     }

# def generate_bin_data(num_bins=50):
#     bins = []
#     for i in range(num_bins):
#         location = generate_bin_location()
#         bins.append({
#             "bin_id": f"bin-{i:03d}",
#             "type": random.choice(["street", "residential", "commercial", "overflow"]),
#             "capacity_kg": random.choice([60, 120, 240, 500]),
#             "installation_date": fake.date_between(start_date='-2y', end_date='today'),
#             "last_maintenance": fake.date_between(start_date='-6m', end_date='today'),
#             "location": location,
#             "status": "active"
#         })
#     return bins

# def generate_citizen_report(bin_id):
#     issues = [
#         "Overflowing bin", "Bad odor", "Recycling contamination",
#         "Damaged bin", "Illegal dumping", "Missed collection"
#     ]
#     return {
#         "report_id": f"report-{datetime.now().strftime('%Y%m%d%H%M%S%f')}",
#         "bin_id": bin_id,
#         "timestamp": datetime.utcnow().isoformat(),
#         "issue_type": random.choice(issues),
#         "description": fake.sentence(),
#         "reporter_id": f"citizen-{random.randint(1000, 9999)}",
#         "status": "reported"
#     }

from faker import Faker
import random
import uuid
from datetime import datetime

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
        bins.append({
            "bin_id": str(uuid.uuid4()), 
            "type": random.choice(["street", "residential", "commercial", "overflow"]),
            "capacity": random.choice([60, 120, 240, 500]), 
            "installation_date": fake.date_between(start_date='-2y', end_date='today'),
            "last_maintenance": fake.date_between(start_date='-6m', end_date='today'),
            "latitude": location["coordinates"]["lat"],
            "longitude": location["coordinates"]["lon"],
            "street": location["street"],
            "district": location["district"],
            "current_fill_level": round(random.uniform(5.0, 75.0), 2), 
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