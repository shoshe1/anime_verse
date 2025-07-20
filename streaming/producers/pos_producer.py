from kafka_producer_base import BaseKafkaProducer
class POSProducer(BaseKafkaProducer):
    def __init__(self):
        super().__init__('pos_transactions.csv', 'POS_topic')
    
    def process_row(self, row):
        if row['quantity_purchased']:
            row['quantity_purchased'] = int(row['quantity_purchased'])
        
        if row['unit_price_at_sale']:
            row['unit_price_at_sale'] = float(row['unit_price_at_sale'])
        
        return row
    
    def get_message_id(self, row):
        return row['transaction_id']
if __name__ == "__main__":
    pos_producer = POSProducer()
    pos_producer.run()