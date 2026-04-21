import json
import random
import time
from datetime import datetime, timedelta
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

product_categories = ["Electronics", "Clothing", "Books", "Home", "Beauty"]
payment_methods = ["CARD", "UPI", "NET_BANKING", "WALLET"]
payment_statuses = ["PENDING", "SUCCESS", "FAILED"]
cities = ["New York", "Boston", "Chicago", "Dallas", "San Francisco"]
states = ["NY", "MA", "IL", "TX", "CA"]

def generate_order(order_num: int):
    quantity = random.randint(1, 5)
    unit_price = round(random.uniform(10, 500), 2)
    total_amount = round(quantity * unit_price, 2)

    # simulate late-arriving data
    delay_seconds = random.choice([0, 0, 0, 30, 60, 120])
    event_time = datetime.utcnow() - timedelta(seconds=delay_seconds)

    event = {
        "event_id": f"EVT-{order_num}",
        "event_type": "ORDER_PLACED",
        "order_id": f"ORD-{1000 + order_num}",
        "customer_id": f"CUST-{random.randint(100, 999)}",
        "product_id": f"PROD-{random.randint(1, 100)}",
        "product_category": random.choice(product_categories),
        "quantity": quantity,
        "unit_price": unit_price,
        "total_amount": total_amount,
        "payment_method": random.choice(payment_methods),
        "payment_status": random.choice(payment_statuses),
        "shipping_city": random.choice(cities),
        "shipping_state": random.choice(states),
        "event_timestamp": event_time.strftime("%Y-%m-%d %H:%M:%S")
    }

    return event

print("Sending fake order events to Kafka... Press Ctrl+C to stop.")

order_num = 1
try:
    while True:
        event = generate_order(order_num)

        # send normal event
        producer.send("ecommerce.orders.raw", value=event)
        print(f"Sent: {event['order_id']} | {event['event_id']} | {event['product_category']} | ${event['total_amount']}")

        # simulate duplicate events sometimes
        if random.random() < 0.15:
            producer.send("ecommerce.orders.raw", value=event)
            print(f"DUPLICATE SENT: {event['order_id']} | {event['event_id']}")

        producer.flush()
        order_num += 1
        time.sleep(1)

except KeyboardInterrupt:
    print("\nStopped producer.")
finally:
    producer.close()