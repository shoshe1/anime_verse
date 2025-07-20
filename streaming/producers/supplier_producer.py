
from kafka_producer_base import BaseKafkaProducer
class SupplierProducer(BaseKafkaProducer):
    def __init__(self):
        super().__init__('supplier_deliveries.csv', 'supllier_topic')
    
    def process_row(self, row):
        if row['quantity_delivered']:
            row['quantity_delivered'] = int(row['quantity_delivered'])
    
        if row['unit_cost']:
             row['unit_cost'] = float(row['unit_cost'])
        return row
    
    def get_message_id(self, row):
        return row['delivery_id']
if __name__ == "__main__":  
    supplier_producer = SupplierProducer()
    supplier_producer.run()