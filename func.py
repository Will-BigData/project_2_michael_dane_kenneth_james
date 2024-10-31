from datetime import datetime, timedelta
import random
from var import *
#custom functions


# Function to randomly generate a product with product_id, product_name, product_category, and price
def generate_product():
    product_id, product_name, product_category, price = random.choice(products)
    return product_id, product_name, product_category, price

# Function to randomly generate a customer with customer_id, customer_name, country, and city
def generate_customers():
    customer_id, customer_name, country, city = random.choice(customers)
    return customer_id, customer_name, country, city


# Helper function to generate random datetime
def random_date(start, end):
    """Generate a random datetime between `start` and `end`."""
    delta = end - start
    random_seconds = random.randint(0, int(delta.total_seconds()))
    return start + timedelta(seconds=random_seconds)

# Set the start and end date for the range
start_date = datetime(2020, 1, 1)
end_date = datetime(2022, 1, 1)