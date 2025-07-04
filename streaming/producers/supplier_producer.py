import csv
import json
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import time
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # e.g. /app/producers
CSV_FILE = os.path.join(BASE_DIR, '..', 'mock_data', 'supplier_deliveries.csv')

    # your processing here

# Configuration
KAFKA_SERVERS = ['localhost:9092']
TOPIC = 'supllier_topic'

# Create producer
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
        if row['quantity_delivered']:
            row['quantity_delivered'] = int(row['quantity_delivered'])
    
        if row['unit_cost']:
            row['unit_cost'] = float(row['unit_cost'])
        
        # Send each row as a message
        producer.send(TOPIC, row)
        print(f"Sent: {row['delivery_id']}")
        time.sleep(1)

# Wait for all messages to be sent
producer.flush()
producer.close()

print("Done!")