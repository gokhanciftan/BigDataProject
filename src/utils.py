from typing import List
from dags.models.models import Event, PurchasedItems, Product
from faker import Faker
import random

fake = Faker()

def generate_event() -> Event:
    event = Event(
        user_id=fake.pyint(1,100),
        session_id=fake.pyint(1,1000),
        event_name=random.choice(["PageVisited", "AddedBasket", "CheckedProductReviews"]),
        time_stamp=fake.date_time_this_month().strftime("%Y-%m-%d %H:%M:%S"),
        attributes={
            "product_id": fake.pyint(1,100),
            "price": random.uniform(50.0, 500.0),
            "discount": fake.pyint(1,25),
            "item_count": fake.pyint(1, 10)
        }
    )
    return event

def generate_purchased_items() -> PurchasedItems:
    purchased_items = PurchasedItems(
        session_id=fake.pyint(1, 1000),
        time_stamp=fake.date_time_this_month().strftime("%Y-%m-%d %H:%M:%S"),
        user_id=fake.pyint(1, 100),
        total_price=random.uniform(50.0, 500.0),
        order_id=fake.pyint(1, 10000),
        payment_type=random.choice(["CreditCard", "PayPal", "Cash"]),
        products=generate_products()
    )
    return purchased_items

def generate_products() -> List[Product]:
    products = []
    for _ in range(fake.random_int(1, 3)):   
        product = Product(
            product_id=fake.random_int(1, 100),
            price=random.uniform(50.0, 500.0),
            discount=fake.random_int(1, 25),
            item_count=fake.random_int(1, 10)
        )
        products.append(product)
    return products
    
