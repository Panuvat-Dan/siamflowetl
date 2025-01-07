import pandas as pd
import random
from faker import Faker

fake = Faker()

# Define the number of rows and columns
num_rows = 100
columns = ["transaction_id", "customer_id", "product_id", "quantity", "transaction_date"]

# Generate data
data = {
    "transaction_id": [fake.uuid4() for _ in range(num_rows)],
    "customer_id": [fake.uuid4() for _ in range(num_rows)],
    "product_id": [fake.uuid4() for _ in range(num_rows)],
    "quantity": [random.randint(1, 100) for _ in range(num_rows)],
    "transaction_date": [fake.date_this_year() for _ in range(num_rows)]
}

# Create DataFrame
df = pd.DataFrame(data, columns=columns)

# Save to CSV
df.to_csv("/C:/Users/sam/Desktop/siamflowetl/data/input/input.csv", index=False)
