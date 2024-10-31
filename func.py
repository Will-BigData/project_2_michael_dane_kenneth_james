from datetime import datetime, timedelta
import random
from var import *
#custom functions


# Function to randomly generate a product with product_id, product_name, product_category, and price
def generate_product(prod):
    product_id, product_name, product_category, price = random.choice(prod)
    return product_id, product_name, product_category, price

# Function to randomly generate a customer with customer_id, customer_name, country, and city
def generate_customers():
    customer_id, customer_name, country, city = random.choice(customers)
    return customer_id, customer_name, country, city

# Helper function to generate random datetime
def random_date():
    """Generate a random datetime between `start` and `end`."""
    # Set the start and end date for the range
    start = datetime(2020, 1, 1)
    end = datetime(2022, 1, 1) 
    delta = end - start
    random_seconds = random.randint(0, int(delta.total_seconds()))
    return start + timedelta(seconds=random_seconds)




# Helper function to generate random datetime in December of specified years
def random_date_in_december():
    """Generate a random datetime in December 2020 or December 2021."""
    # Choose a random year between 2020 and 2021
    year = random.choice([2020, 2021])
    
    # Define the start and end of December for the chosen year
    start_date = datetime(year, 12, 1)
    end_date = datetime(year, 12, 31, 23, 59, 59)
    
    # Calculate the time delta in seconds for the month
    delta = end_date - start_date
    random_seconds = random.randint(0, int(delta.total_seconds()))
    
    return start_date + timedelta(seconds=random_seconds)


# Check if city is on the list of bulk buyers
def check_city(city):
    bulk_buyers = ["Chicago", "Vancouver", "Los Angeles", "Berlin"]

    for buyer in bulk_buyers:
        if buyer == city:
            return random.randint(4,7)
    
    return 1

#returns random payment type
def rand_payment_type():
    return random.choice(["Card", "Internet Banking", "UPI", "Wallet"])

#returns specific payment type for trend
def payment_type():
    return "Internet Banking"