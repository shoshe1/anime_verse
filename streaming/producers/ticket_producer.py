import csv
import json
import time
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

import os
BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # e.g. /app/producers
CSV_FILE = os.path.join(BASE_DIR, '..', 'mock_data', 'ticket_bookings.csv')
# Configuration
KAFKA_SERVERS = ['localhost:9092']
TOPIC = 'bookings_topic'

while True:
    try:
        producer = KafkaProducer(
            bootstrap_servers="kafka:9092",
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        break
    except NoBrokersAvailable:
        print("Kafka not available, retrying in 5 seconds...")
        time.sleep(5)

# Read CSV and send messages
with open(CSV_FILE, 'r') as file:
    reader = csv.DictReader(file)
    
    for row in reader:
        # Convert numeric columns
        if row['quantity_purchased']:
            row['quantity_purchased'] = int(row['quantity_purchased'])
        if row['ticket_quantity']:
            row['ticket_quantity'] = int(row['ticket_quantity'])
        if row['unit_price_at_sale']:
            row['unit_price_at_sale'] = float(row['unit_price_at_sale'])
        
        # Send each row as a message
        producer.send(TOPIC, row)
        print(f"Sent: {row['booking_id']}")
        time.sleep(1)

# Wait for all messages to be sent
producer.flush()
producer.close()

print("Done!")