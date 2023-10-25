# Importing necessary libraries
import pandas as pd
import random
import os
import json
from datetime import datetime, timedelta, time
import uuid
from enum import Enum
import pyarrow as pa
import pyarrow.parquet as pq
from decimal import Decimal


# Number of rows to generate
n_rows = 100

class Color(Enum):
    RED = 1
    GREEN = 2
    BLUE = 3

int32_col = pd.Series(range(n_rows), dtype='int32')
int64_col = pd.Series(range(n_rows), dtype='int64')
bool_col = pd.Series([random.choice([True, False]) for _ in range(n_rows)], dtype='bool')
string_col = pd.Series(['string_' + str(i) for i in range(n_rows)], dtype='string')
json_col = pd.Series([json.dumps({'key': random.randint(1, 10)}) for _ in range(n_rows)], dtype='string')
date_col = pd.Series([datetime.now().date() + timedelta(days=i) for i in range(n_rows)], dtype='object')
timestamp_col = pd.Series([pd.Timestamp(datetime.now() + timedelta(seconds=i * 10)) for i in range(n_rows)], dtype='datetime64[ns]')
time_col = pd.Series([time(hour=(i+1) % 24, minute=((i+1) * 2) % 60, second=((i+1) * 3) % 60) for i in range(n_rows)], dtype='object')
uuid_col = pd.Series([str(uuid.uuid4()) for _ in range(n_rows)], dtype='string')
enum_col = pd.Series([random.choice(list(Color)).name for _ in range(n_rows)], dtype='string')
float_col = pd.Series([random.uniform(0, 100) for _ in range(n_rows)], dtype='float32')
double_col = pd.Series([random.uniform(0, 100) for _ in range(n_rows)], dtype='float64')
decimal_col = pd.Series([Decimal(str(round(random.uniform(0, 100), 2))) for i in range(n_rows)])

int32_nullable_col = pd.Series([i if i % 2 == 0 else None for i in range(n_rows)], dtype='Int32')
int64_nullable_col = pd.Series([i if i % 2 == 0 else None for i in range(n_rows)], dtype='Int64')
bool_nullable_col = pd.Series([True if i % 2 == 0 else None for i in range(n_rows)], dtype='boolean')
string_nullable_col = pd.Series(['string_' + str(i) if i % 2 == 0 else None for i in range(n_rows)], dtype='string')
json_nullable_col = pd.Series([json.dumps({'key': random.randint(1, 10)}) if i % 2 == 0 else None for i in range(n_rows)], dtype='string')
date_nullable_col = pd.Series([datetime.now().date() + timedelta(days=i) if i % 2 == 0 else None for i in range(n_rows)], dtype='object')
timestamp_nullable_col = pd.Series([pd.Timestamp(datetime.now() + timedelta(seconds=i * 10)) if i % 2 == 0 else None for i in range(n_rows)], dtype='object')
time_nullable_col = pd.Series([time(hour=(i+1) % 24, minute=((i+1) * 2) % 60, second=((i+1) * 3) % 60) if i % 2 == 0 else None for i in range(n_rows)], dtype='object')
uuid_nullable_col = pd.Series([str(uuid.uuid4()) if i % 2 == 0 else None for i in range(n_rows)], dtype='string')
enum_nullable_col = pd.Series([random.choice(list(Color)).name if i % 2 == 0 else None for i in range(n_rows)], dtype='string')
float_nullable_col = pd.Series([random.uniform(0, 100) if i % 2 == 0 else None for i in range(n_rows)], dtype='float32')
double_nullable_col = pd.Series([random.uniform(0, 100) if i % 2 == 0 else None for i in range(n_rows)], dtype='float64')
decimal_nullable_col = pd.Series([Decimal(str(round(random.uniform(0, 100), 2))) if i % 2 == 0 else None for i in range(n_rows)])

# Creating the DataFrame with only the new column
df_nested_list = pd.DataFrame({
    'int32': int32_col,
    'int32_nullable': int32_nullable_col,
    'int64': int64_col,
    'int64_nullable': int64_nullable_col,
    'bool': bool_col,
    'bool_nullable': bool_nullable_col,
    'string': string_col,
    'string_nullable': string_nullable_col,
    'json': json_col,
    'json_nullable': json_nullable_col,
    'date': date_col,
    'date_nullable': date_nullable_col,
    'timestamp': timestamp_col,
    'timestamp_nullable': timestamp_nullable_col,
    'time': time_col,
    'time_nullable': time_nullable_col,
    'uuid': uuid_col,
    'uuid_nullable': uuid_nullable_col,
    'enum': enum_col,
    'enum_nullable': enum_nullable_col,
    'float': float_col,
    'float_nullable': float_nullable_col,
    'double': double_col,
    'double_nullable': double_nullable_col,
    'decimal': decimal_col,
    'decimal_nullable': decimal_nullable_col,
})

# Define the schema
schema = pa.schema([
    ('int32', pa.int32()),
    ('int32_nullable', pa.int32()),
    ('int64', pa.int64()),
    ('int64_nullable', pa.int64()),
    ('bool', pa.bool_()),
    ('bool_nullable', pa.bool_()),
    ('string', pa.string()),
    ('string_nullable', pa.string()),
    ('json', pa.string()),
    ('json_nullable', pa.string()),
    ('date', pa.date32()),
    ('date_nullable', pa.date32()),
    ('timestamp', pa.timestamp('ns')),
    ('timestamp_nullable', pa.timestamp('ns')),
    ('time', pa.time64('ns')),
    ('time_nullable', pa.time64('ns')),
    ('uuid', pa.string()),
    ('uuid_nullable', pa.string()),
    ('enum', pa.string()),
    ('enum_nullable', pa.string()),
    ('float', pa.float32()),
    ('float_nullable', pa.float32()),
    ('double', pa.float64()),
    ('double_nullable', pa.float64()),
    ('decimal', pa.decimal128(10, 2)),
    ('decimal_nullable', pa.decimal128(10, 2)),
])

# Create a PyArrow Table
table = pa.Table.from_pandas(df_nested_list, schema=schema)

# Define the Parquet file path
parquet_file = 'output/primitives.parquet'

# Check if the file exists and remove it
if os.path.exists(parquet_file):
    os.remove(parquet_file)

# Write the PyArrow Table to a Parquet file
with pq.ParquetWriter(parquet_file, schema, compression='SNAPPY') as writer:
    writer.write_table(table)

pd.set_option('display.max_columns', None)  # Show all columns
pd.set_option('display.max_rows', None)     # Show all rows
pd.set_option('display.width', None)        # Auto-detect the width for displaying
pd.set_option('display.max_colwidth', None) # Show complete text in each cell

# Show the first few rows of the DataFrame for verification
print(df_nested_list.head(10))
