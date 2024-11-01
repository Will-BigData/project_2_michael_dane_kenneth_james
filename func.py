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


# Helper function to generate random datetime between June 1 and August 31 of specified years
def random_date_in_summer():
    """Generate a random datetime between June 1 and August 31, 2020 or 2021."""
    # Choose a random year between 2020 and 2021
    year = random.choice([2020, 2021])
    
    # Define the start and end of summer for the chosen year
    start_date = datetime(year, 6, 1)
    end_date = datetime(year, 8, 31, 23, 59, 59)
    
    # Calculate the time delta in seconds for the period
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





# Helper function to generate random datetime
def random_date(start, end):
    """Generate a random datetime between `start` and `end`."""
    delta = end - start
    random_seconds = random.randint(0, int(delta.total_seconds()))
    return start + timedelta(seconds=random_seconds)


# Set the start and end date for the range
start_date = datetime(2020, 1, 1)
end_date = datetime(2022, 1, 1)

# Lambda function to call random_date with the specified range
date_generator = lambda: random_date(start_date, end_date)

# Helper function to generate random datetime for December 2020 or December 2021
def december_date():
    """Generate a random datetime within December 2020 or December 2021."""
    # Define date ranges for December 2020 and December 2021
    start_date_2020 = datetime(2020, 12, 1)
    end_date_2020 = datetime(2020, 12, 31, 23, 59, 59)
    
    start_date_2021 = datetime(2021, 12, 1)
    end_date_2021 = datetime(2021, 12, 31, 23, 59, 59)
    
    # Randomly choose between December 2020 and December 2021
    if random.choice([True, False]):
        start, end = start_date_2020, end_date_2020
    else:
        start, end = start_date_2021, end_date_2021
    
    # Generate a random datetime within the chosen December
    delta = end - start
    random_seconds = random.randint(0, int(delta.total_seconds()))
    return start + timedelta(seconds=random_seconds)