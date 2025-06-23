# streaming/pos_producer.py
from kafka import KafkaProducer
import json
import time
import uuid
from datetime import datetime, timezone
import random

producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

while True:
    message = {
        "transaction_id": str(uuid.uuid4()),
        "event_ts": datetime.now(timezone.utc).isoformat(),
        "ingestion_ts": datetime.now(timezone.utc).isoformat(),
        "customer_id": f"C{random.randint(1, 100)}",
        "store_id": f"S{random.randint(1, 5)}",
        "product_id": f"P{random.randint(1, 20)}",
        "quantity_purchased": random.randint(1, 5),
        "unit_price_at_sale": round(random.uniform(5.0, 50.0), 2)
    }

    producer.send("pos-transactions", value=message)
    print(f"Sent: {message}")
    time.sleep(2)
