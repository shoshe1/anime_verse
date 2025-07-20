
from kafka_producer_base import BaseKafkaProducer
class TicketProducer(BaseKafkaProducer):
    def __init__(self):
        super().__init__('ticket_bookings.csv', 'bookings_topic')
    
    def process_row(self, row):
        if row['quantity_purchased']:
            row['quantity_purchased'] = int(row['quantity_purchased'])
        if row['ticket_quantity']:
            row['ticket_quantity'] = int(row['ticket_quantity'])
        if row['unit_price_at_sale']:
            row['unit_price_at_sale'] = float(row['unit_price_at_sale'])
        
        return row
    
    def get_message_id(self, row):
        return row['booking_id']
if __name__ == "__main__":  
    ticket_producer = TicketProducer()
    ticket_producer.run()