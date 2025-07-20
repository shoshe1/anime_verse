import csv
import json
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import time
import random
from datetime import datetime, timedelta, timezone
import os
from abc import ABC, abstractmethod

class BaseKafkaProducer(ABC):
    def __init__(self, csv_filename, topic, bootstrap_servers="kafka:9092"):
        self.base_dir = os.path.dirname(os.path.abspath(__file__))
        self.csv_file = os.path.join(self.base_dir, '..', 'mock_data', csv_filename)
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        self.producer = None
        self.rows = []
        
    def create_producer(self):
        """Create Kafka producer with retry logic"""
        while True:
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=self.bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode("utf-8")
                )
                break
            except NoBrokersAvailable:
                print("Kafka not available, retrying in 5 seconds...")
                time.sleep(5)
    
    def load_csv_data(self):
        """Load CSV data into memory"""
        with open(self.csv_file, 'r') as file:
            self.rows = list(csv.DictReader(file))
        print(f"Loaded {len(self.rows)} rows from CSV.")
    
    def add_timestamps(self, row,max_delay_seconds=300):
        """Add event_time and ingestion_time to row"""
        now = datetime.now(timezone.utc)
        event_time = now - timedelta(seconds=random.randint(0, max_delay_seconds))
        row['event_ts'] = event_time.isoformat()
        row['ingestion_ts'] = now.isoformat()
        return row
    
    @abstractmethod
    def process_row(self, row):
        """Process row data - to be implemented by subclasses"""
        pass
    
    @abstractmethod
    def get_message_id(self, row):
        """Get message ID for logging - to be implemented by subclasses"""
        pass
    
    def run(self,max_delay_seconds=300):
        """Main producer loop"""
        self.create_producer()
        self.load_csv_data()
        
        try:
            while True:
                row = random.choice(self.rows)
                processed_row = self.process_row(row.copy())
                processed_row = self.add_timestamps(processed_row,max_delay_seconds)
                
                self.producer.send(self.topic, processed_row)
                print(f"Sent: {self.get_message_id(processed_row)}")
                time.sleep(random.uniform(0.2, 0.5))
                
        except KeyboardInterrupt:
            print("Stopped by user.")
        finally:
            if self.producer:
                self.producer.flush()
                self.producer.close()
            print("Done!")

