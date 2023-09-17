import pandas as pd
import random
import os
import pyarrow as pa
import pyarrow.parquet as pq

# Number of rows to generate
n_rows = 100

# Functions to generate the data
def generate_list_nested():
    return [
        [
            [
                random.randint(1, 10) for _ in range(random.randint(1, 3))
            ] for _ in range(random.randint(1, 3))
        ] for _ in range(random.randint(1, 3))
    ]

# Columns
list_col = pd.Series([[random.randint(1, 10) for _ in range(3)] for _ in range(n_rows)], dtype='object')
list_nullable_col = pd.Series([[random.randint(1, 10) for _ in range(3)] if i % 2 == 0 else None for i in range(n_rows)], dtype='object')
list_mixed_types_col = pd.Series([
    [
        {'int': i, 'string': None, 'bool': None},
        {'int': None, 'string': "string_" + str(i), 'bool': None},
        {'int': None, 'string': None, 'bool': bool(i % 2)},
        {'int': None, 'string': None, 'bool': None}
    ] for i in range(n_rows)
], dtype='object')
list_nested_col = pd.Series([generate_list_nested() for _ in range(n_rows)], dtype='object')

# Creating the DataFrame with only the new column
df_nested_list = pd.DataFrame({
    'list': list_col,
    'list_nullable': list_nullable_col,
    'list_mixed_types': list_mixed_types_col,
    'list_nested': list_nested_col
})

# Types
list_type = pa.list_(pa.int32())
list_mixed_type = pa.list_(
    pa.struct([
        pa.field('int', pa.int32()),
        pa.field('string', pa.string()),
        pa.field('bool', pa.bool_())
    ])
)
list_nested_type = pa.list_(pa.list_(pa.list_(pa.int32())))

# Define the schema
schema = pa.schema([
    ('list', list_type),
    ('list_nullable', list_type),
    ('list_mixed_types', list_mixed_type),
    ('list_nested', list_nested_type),
])

parquet_file = 'output/lists.parquet'
# Create a PyArrow Table
table = pa.Table.from_pandas(df_nested_list, schema=schema)

# Check if the file exists and remove it
if os.path.exists(parquet_file):
    os.remove(parquet_file)

# Write the PyArrow Table to a Parquet file
with pq.ParquetWriter(parquet_file, schema, compression='GZIP') as writer:
    writer.write_table(table)

pd.set_option('display.max_columns', None)  # Show all columns
pd.set_option('display.max_rows', None)     # Show all rows
pd.set_option('display.width', None)        # Auto-detect the width for displaying
pd.set_option('display.max_colwidth', None) # Show complete text in each cell

# Show the first few rows of the DataFrame for verification
print(df_nested_list.head(10))
