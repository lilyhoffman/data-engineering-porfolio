import json
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer


KAFKA_BROKER = "localhost:9092"
TOPIC = "orders"

PRODUCTS = [
    ("Electronics", 199.99),
    ("Clothing", 49.99),
    ("Home", 89.99),
    ("Books", 19.99),
    ("Sports", 69.99),
]

PAYMENT_METHODS = [
    "credit_card",
    "debit_card",
    "paypal",
]

STATUSES = [
    "completed",
    "completed",
    "completed",
    "cancelled",
]


def generate_order():
    category, unit_price = random.choice(PRODUCTS)

    return {
        "order_id": str(uuid.uuid4()),
        "customer_id": f"CUST-{random.randint(100, 999)}",
        "product_id": f"PROD-{random.randint(1, 100)}",
        "category": category,
        "quantity": random.randint(1, 5),
        "unit_price": unit_price,
        "order_timestamp": datetime.now(timezone.utc).isoformat(),
        "payment_method": random.choice(PAYMENT_METHODS),
        "status": random.choice(STATUSES),
    }


producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
)

print("Starting order producer...")

try:
    while True:
        order = generate_order()

        producer.send(TOPIC, value=order)
        producer.flush()

        print(f"Sent order: {order}")

        time.sleep(2)

except KeyboardInterrupt:
    print("\nStopping producer.")

finally:
    producer.close()