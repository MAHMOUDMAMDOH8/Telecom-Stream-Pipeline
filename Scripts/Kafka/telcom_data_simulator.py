import random
import time
import json
from datetime import datetime, timedelta


def load_customers(filepath="dim_user.json"):
    with open(filepath, "r") as f:
        users = json.load(f)

        for idx, user in enumerate(users, start=1001):
            user.setdefault('customer_id', idx)
            user.setdefault('plan_type', random.choice(["Prepaid", "Postpaid"]))
            user.setdefault('status', random.choice(["Active", "Inactive", "Suspended"]))
        return users

def load_cell_sites(filepath="dim_cell_site.json"):
    with open(filepath, "r") as f:
        return json.load(f)

tac_to_manufacturer = {
    "35846279": "Samsung",
    "35846270": "Apple",
    "86945303": "Huawei",
    "35846103": "Xiaomi",
    "35944803": "Oppo"
}
tac_codes = list(tac_to_manufacturer.keys())


def generate_imei_with_manufacturer():
    tac = random.choice(tac_codes)
    serial = ''.join([str(random.randint(0, 9)) for _ in range(7)])
    imei = tac + serial
    return imei, tac_to_manufacturer[tac]


def generate_user_info(customers, cell_sites):
    customer = random.choice(customers)
    imei, manufacturer = generate_imei_with_manufacturer()
    return {
        "number": customer["phone_number"],
        "cell_site": random.choice(cell_sites)["cell_id"],
        "imei": imei,
        "customer_id": customer["customer_id"],  
        "plan_type": customer["plan_type"]  
    }


sample_messages = [
    "Hello!",
    "How are you?",
    "Your verification code is 123456",
    "Meeting at 5PM",
    "Congratulations! You've won!",
    "Please call me back",
    "See you tomorrow",
    "Important update regarding your account"
]

def generate_sms_event(customers, cell_sites):
    sms_status = random.choice(["queued", "sending", "sent", "delivered", "failed"])


    if sms_status in ["sent", "delivered"]:
        billing_amount = round(random.uniform(0.5, 5.0), 2)
    else:
        billing_amount = 0

    event = {
        "event_type": "sms",
        "sid": f"SM{random.randint(10**8, 10**9)}",
        "from": generate_user_info(customers, cell_sites),
        "to": generate_user_info(customers, cell_sites),
        "body": random.choice(sample_messages),
        "status": sms_status,
        "timestamp": datetime.now().strftime("%d-%m-%Y %H:%M:%S"),
        "customer_id": generate_user_info(customers, cell_sites)["customer_id"],
        "registration_date": (datetime.now() - timedelta(days=random.randint(1, 365))).strftime("%Y-%m-%d"),
        "billing_info": {
            "amount": billing_amount,
            "currency": "EGP"
        }
    }
    return event



def generate_call_event(customers, cell_sites):
    call_status = random.choice(["initiated", "ringing", "in-progress", "completed", "failed", "busy", "no-answer"])


    if call_status in ["ringing", "in-progress", "completed"]:
        call_duration  = round(random.uniform(1.0, 60.0), 2)
        call_duration_sec = call_duration*60
        billing_amount = call_duration * .16
    else:
        call_duration = 0
        call_duration_sec=0
        billing_amount = 0

    event = {
        "event_type": "call",
        "sid": f"CA{random.randint(10**8, 10**9)}",
        "from": generate_user_info(customers, cell_sites),
        "to": generate_user_info(customers, cell_sites),
        "call_duration_seconds": call_duration,
        "status": call_status,
        "timestamp": datetime.now().strftime("%d-%m-%Y %H:%M:%S"),
        "call_type": random.choice(["Local", "International"]),
        "billing_info": {
            "amount": billing_amount,
            "currency": "EGP"
        }
    }
    return event

def main(num_events=10):  
    customers = load_customers('/opt/airflow/scripts/Kafka/dim_user.json')
    cell_sites = load_cell_sites('/opt/airflow/scripts/Kafka/dim_cell_site.json')
    
    all_events = []
    counter = 0
    for _ in range(num_events):  
        event_type = random.choice(["sms", "call"])
        event = generate_sms_event(customers, cell_sites) if event_type == "sms" else generate_call_event(customers, cell_sites)
        all_events.append(event)
        print(f"[{counter + 1:03}] Generated {event_type.upper()} — From {event['from']['number']} To {event['to']['number']} on Cell Site {event['from']['cell_site']}")
        counter +=1
        time.sleep(random.uniform(0.1, 0.5))
    
    return all_events