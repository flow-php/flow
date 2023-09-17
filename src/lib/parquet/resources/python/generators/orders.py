import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from faker import Faker
import uuid
from datetime import datetime
import random

# Initialize Faker
fake = Faker()

# Number of rows you want in your Parquet file
num_rows = 100000

# Generate data
order_ids = [str(uuid.uuid4()) for _ in range(num_rows)]
total_prices = [round(random.uniform(50.0, 200.0), 2) for _ in range(num_rows)]
discounts = [round(random.uniform(0.0, 50.0), 2) for _ in range(num_rows)]
created_at = [datetime.now() for _ in range(num_rows)]
updated_at = [datetime.now() for _ in range(num_rows)]

customers = [{'customer_id': str(uuid.uuid4()), 'first_name': fake.first_name(), 'last_name': fake.last_name(), 'email': fake.email()} for _ in range(num_rows)]

addresses = [{'address_id': str(uuid.uuid4()), 'street': fake.street_address(), 'city': fake.city(), 'state': fake.state(), 'zip': fake.zipcode(), 'country': fake.country()} for _ in range(num_rows)]

order_lines = [[{'order_line_id': str(uuid.uuid4()), 'product_id': str(uuid.uuid4()), 'quantity': random.randint(1, 10), 'price': round(random.uniform(1.0, 50.0), 2)} for _ in range(random.randint(1, 5))] for _ in range(num_rows)]

notes = [[{'note_id': str(uuid.uuid4()), 'note_text': fake.text()} for _ in range(random.randint(1, 3))] for _ in range(num_rows)]

# Create a DataFrame
df = pd.DataFrame({
    'order_id': order_ids,
    'total_price': total_prices,
    'discount': discounts,
    'created_at': created_at,
    'updated_at': updated_at,
    'customer': customers,
    'address': addresses,
    'order_lines': order_lines,
    'notes': notes
})

# Define schema
schema = pa.schema([
    ('order_id', pa.string()),
    ('total_price', pa.float32()),
    ('discount', pa.float32()),
    ('created_at', pa.timestamp('ns')),
    ('updated_at', pa.timestamp('ns')),
    ('customer', pa.struct([
        ('customer_id', pa.string()),
        ('first_name', pa.string()),
        ('last_name', pa.string()),
        ('email', pa.string())
    ])),
    ('address', pa.struct([
        ('address_id', pa.string()),
        ('street', pa.string()),
        ('city', pa.string()),
        ('state', pa.string()),
        ('zip', pa.string()),
        ('country', pa.string())
    ])),
    ('order_lines', pa.list_(
        pa.struct([
            ('order_line_id', pa.string()),
            ('product_id', pa.string()),
            ('quantity', pa.int32()),
            ('price', pa.float32())
        ])
    )),
    ('notes', pa.list_(
        pa.struct([
            ('note_id', pa.string()),
            ('note_text', pa.string())
        ])
    ))
])

# Convert DataFrame to PyArrow Table
table = pa.table(df, schema=schema)

# Write out as Parquet file with Snappy compression
pq.write_table(table, 'output/orders.parquet', compression='gzip')
