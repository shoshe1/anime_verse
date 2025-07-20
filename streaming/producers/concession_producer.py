from kafka_producer_base import BaseKafkaProducer

class ConcessionProducer(BaseKafkaProducer):
    def __init__(self):
        super().__init__('concession_purchases.csv', 'concession_topic')
    
    def process_row(self, row):
        if row['item_quantity']:
            row['item_quantity'] = int(row['item_quantity'])
        
        if row['item_unit_price']:
            row['item_unit_price'] = float(row['item_unit_price'])
        
        return row
    
    def get_message_id(self, row):
        return row['purchase_id']
    

if __name__ == "__main__":
    concession_producer = ConcessionProducer()
    concession_producer.run()