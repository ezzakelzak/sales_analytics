import json
import string
import random
import time
from datetime import datetime, timedelta
from faker import Faker
from google.cloud import pubsub_v1
import os
from dotenv import load_dotenv

fake = Faker()
load_dotenv()

PROJECT_ID = os.getenv("PROJECT_ID")
TOPIC_ID = os.getenv("TOPIC_ID")

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)


PRODUCTS = [
    
    {"product_name": "ASUS ROG Strix G15", "category": "computer", "unit_price": 1299.99},
    {"product_name": "Dell XPS 13", "category": "computer", "unit_price": 1099.99},
    {"product_name": "HP Pavilion 15", "category": "computer", "unit_price": 799.99},
    {"product_name": "Lenovo ThinkPad X1", "category": "computer", "unit_price": 1499.99},
    {"product_name": "MacBook Air M2", "category": "computer", "unit_price": 1199.99},
    {"product_name": "Acer Aspire 5", "category": "computer", "unit_price": 649.99},
    
    # Watches
    {"product_name": "Apple Watch Series 9", "category": "watch", "unit_price": 429.99},
    {"product_name": "Samsung Galaxy Watch 6", "category": "watch", "unit_price": 349.99},
    {"product_name": "Garmin Forerunner 255", "category": "watch", "unit_price": 399.99},
    {"product_name": "Fitbit Sense 2", "category": "watch", "unit_price": 249.99},
    {"product_name": "Xiaomi Mi Watch", "category": "watch", "unit_price": 149.99},
    
    # Headphones
    {"product_name": "Sony WH-1000XM5", "category": "headphones", "unit_price": 399.99},
    {"product_name": "Bose QuietComfort 45", "category": "headphones", "unit_price": 329.99},
    {"product_name": "AirPods Pro 2", "category": "headphones", "unit_price": 249.99},
    {"product_name": "JBL Tune 760NC", "category": "headphones", "unit_price": 129.99},
    {"product_name": "Sennheiser HD 450BT", "category": "headphones", "unit_price": 179.99},
    {"product_name": "Beats Studio Pro", "category": "headphones", "unit_price": 349.99},
    
    # Cameras
    {"product_name": "Canon EOS R6", "category": "camera", "unit_price": 2499.99},
    {"product_name": "Nikon Z6 II", "category": "camera", "unit_price": 1999.99},
    {"product_name": "Sony Alpha A7 IV", "category": "camera", "unit_price": 2499.99},
    {"product_name": "Fujifilm X-T4", "category": "camera", "unit_price": 1699.99},
    {"product_name": "Panasonic Lumix GH5", "category": "camera", "unit_price": 1399.99},
    {"product_name": "GoPro Hero 12", "category": "camera", "unit_price": 399.99},
]

CHANNELS = ["online", "store"]
PAYMENTS = ["credit_card", "paypal", "cash", "apple_pay"]
LOYALTY = ["none", "silver", "gold", "platinum"]
COUNTRIES = ["FR", "ES", "IT", "US", "UK", "JP"]
STORES = ["S01", "S02", "S03", "S04", "S05"]


COUNTRY_CURRENCY = {
    "FR": "EUR",
    "ES": "EUR",
    "IT": "EUR",
    "US": "USD",
    "UK": "GBP",
    "JP": "JPY"
}

# Taux de conversion approximatifs (base EUR)
CURRENCY_RATES = {
    "EUR": 1.0,
    "USD": 1.10,
    "GBP": 0.85,
    "JPY": 160.0
}


def short_id(length=3):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))


def short_timestamp():
    return (datetime.utcnow() + timedelta(days=0)).strftime("%Y-%m-%dT%H:%M:%S")


def convert_price(price_eur, currency):
    rate = CURRENCY_RATES.get(currency, 1.0)
    converted = price_eur * rate
    
    if currency == "JPY":
        return round(converted, 0)  
    else:
        return round(converted, 2)


def generate_event():
    product = random.choice(PRODUCTS)
    qty = random.randint(1, 2)
    country = random.choice(COUNTRIES)
    currency = COUNTRY_CURRENCY[country]
    
    
    unit_price = convert_price(product["unit_price"], currency)
    total_amount = round(unit_price * qty, 2 if currency != "JPY" else 0)
    
    return {
        "event_id": short_id(),
        "event_timestamp": short_timestamp(),
        "sale": {
            "sale_id": short_id(),
            "sale_timestamp": short_timestamp(),
            "store_id": random.choice(STORES),
            "channel": random.choice(CHANNELS),
            "payment_method": random.choice(PAYMENTS),
            "currency": currency,
            "total_amount": total_amount,
        },
        "customer": {
            "customer_id": random.randint(1000, 99999),
            "loyalty_level": random.choice(LOYALTY),
            "age": random.randint(18, 70),
            "country": country,
        },
        "items": [
            {
                "product_id": short_id(),
                "product_name": product["product_name"],
                "quantity": qty,
                "unit_price": unit_price,
                "category": product["category"],
            }
        ]
    }

# Publier sur Pub/Sub
def publish_event():
    event = generate_event()
    data = json.dumps(event).encode("utf-8")
    publisher.publish(topic_path, data)
    print("Published event:", event)


if __name__ == "__main__":
    while True:
        publish_event()
        time.sleep(10)