import pandas as pd
import random
import string
from datetime import datetime, timedelta

# class for E-commerce data generator
class EcommerceDataGenerator:
    def __init__(self, record_count):
        self.record_count = record_count
        self.names = ["John Smith", "Mary Jane", "Joe Smith", "Neo", "Trinity"]
        self.products = [
            {"id": 201, "name": "Pen", "category": "Stationery"},
            {"id": 202, "name": "Pencil", "category": "Stationery"},
            {"id": 203, "name": "Mobile", "category": "Electronics"},
            {"id": 204, "name": "Laptop", "category": "Electronics"},
            {"id": 205, "name": "Book", "category": "Books"}
        ]
        self.countries_cities = [
            {"country": "India", "city": "Mumbai"},
            {"country": "USA", "city": "Boston"},
            {"country": "UK", "city": "Oxford"},
            {"country": "India", "city": "Indore"},
            {"country": "India", "city": "Bengaluru"}
        ]
        self.payment_types = ["Card", "Internet Banking", "UPI", "Wallet"]
        self.websites = ["www.amazon.com", "www.flipkart.com", "www.tatacliq.com", "www.ebay.in"]
        self.data = []

    def random_date(self):
        start_date = datetime(2021, 1, 1)
        end_date = datetime(2021, 12, 31)
        return start_date + timedelta(seconds=random.randint(0, int((end_date - start_date).total_seconds())))

    def random_txn_id(self):
        return ''.join(random.choices(string.digits, k=5))

    def generate_record(self, order_id):
        customer = random.choice(self.names)
        product = random.choice(self.products)
        location = random.choice(self.countries_cities)
        
        # Set rogue data (5% chance)
        payment_success = "Y" if random.random() > 0.05 else "N"
        failure_reason = "Invalid CVV" if payment_success == "N" else None

        return {
            "order_id": order_id,
            "customer_id": random.randint(100, 500),
            "customer_name": customer,
            "product_id": product["id"],
            "product_name": product["name"],
            "product_category": product["category"],
            "payment_type": random.choice(self.payment_types),
            "qty": random.randint(1, 5),
            "price": round(random.uniform(5.0, 500.0), 2),
            "datetime": self.random_date(),
            "country": location["country"],
            "city": location["city"],
            "ecommerce_website_name": random.choice(self.websites),
            "payment_txn_id": self.random_txn_id(),
            "payment_txn_success": payment_success,
            "failure_reason": failure_reason
        }

    def generate_data(self):
        for i in range(self.record_count):
            record = self.generate_record(i + 1)
            self.data.append(record)
        return self.data

    def save_to_csv(self, filename="ecommerce_data.csv"):
        df = pd.DataFrame(self.data)
        df.to_csv(filename, index=False)
        print(f"Data saved to {filename}")


# Instantiate and run the generator
if __name__ == "__main__":
    record_count = 10000  # Define how many records you want
    generator = EcommerceDataGenerator(record_count)
    generator.generate_data()
    generator.save_to_csv()

