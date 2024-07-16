from typing import List
from pydantic import BaseModel

class Product(BaseModel):
    product_id:int
    price:float
    discount:int
    item_count:int
    
class Event(BaseModel):
    user_id:int
    session_id:int
    event_name:str
    time_stamp:str
    attributes:Product
    
class PurchasedItems(BaseModel):
    session_id:int
    time_stamp:str
    user_id:int
    total_price:float
    order_id:int
    payment_type:str
    products:List[Product]
