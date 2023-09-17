import pandas as pd
import random
import os
import pyarrow as pa
import pyarrow.parquet as pq
import sys

pd.set_option('display.max_columns', None)  # Show all columns
pd.set_option('display.max_rows', None)     # Show all rows
pd.set_option('display.width', None)        # Auto-detect the width for displaying
pd.set_option('display.max_colwidth', None) # Show complete text in each cell

# Number of rows to generate
n_rows = 100

# Functions to generate the data
def generate_map_of_maps():
    return {
        f'outer_key_{i}': {
            f'inner_key_{j}': random.randint(1, 10)
            for j in range(random.randint(1, 3))
        }
        for i in range(random.randint(1, 3))
    }

def generate_map_complex_nested_list():
    return [
        [
            {
                'int': random.randint(1, 10),
                'string': f'string_{i}_{j}'
            }
            for j in range(random.randint(1, 3))
        ]
        for i in range(random.randint(1, 3))
    ]

def generate_map_of_lists():
    return {f'key_{i}': [random.randint(1, 10) for _ in range(random.randint(1, 3))] for i in range(random.randint(1, 3))}

def generate_map_of_complex_lists():
    return {
        f'key_{i}': [
            {
                'int': random.randint(1, 10),
                'string': f'string_{i}_{j}',
                'bool': bool(random.getrandbits(1))
            }
            for j in range(random.randint(1, 3))
        ]
        for i in range(random.randint(1, 3))
    }

def generate_map_of_list_of_map_of_lists():
    return {
        f'key_{i}': [
            {
                f'string_{i}_{j}_{k}': [random.randint(1, 10) for _ in range(random.randint(1, 3))]
                for k in range(random.randint(1, 3))
            }
            for j in range(random.randint(1, 3))
        ]
        for i in range(random.randint(1, 3))
    }

def generate_map_of_structs():
    map_of_structs_data = []
    for i in range(n_rows):
        # Generating a map where each value is a struct with an Int32 and a String field
        map_of_structs_value = {
            f'key_{j}': {
                'int_field': j,
                'string_field': f'string_{j}'
            } for j in range(3)
        }
        map_of_structs_data.append(map_of_structs_value)
    return map_of_structs_data

def generate_map_of_struct_of_structs(n_rows):
    map_of_struct_of_structs_data = []  # List to hold all the data
    for i in range(n_rows):
        map_of_struct_of_structs_value = {
            f'key_{j}': {
                'struct': {
                    'nested_struct': {
                        'int': random.randint(1, 100),
                        'string': f'string_{j}'
                    }
                }
            } for j in range(3)  # Creating 3 key-value pairs in each map
        }
        map_of_struct_of_structs_data.append(map_of_struct_of_structs_value)
    return map_of_struct_of_structs_data

# Columns
map_col = [{"key_" + str(i): i} for i in range(n_rows)]
map_nullable_col = pd.Series([{"key_" + str(i): i} if i % 2 == 0 else None for i in range(n_rows)], dtype='object')
map_of_maps_col = pd.Series([generate_map_of_maps() for _ in range(n_rows)], dtype='object')
map_of_lists_col = pd.Series([generate_map_of_lists() for _ in range(n_rows)], dtype='object')
map_of_complex_lists_col = pd.Series([generate_map_of_complex_lists() for _ in range(n_rows)], dtype='object')
map_of_list_of_map_of_lists_col = pd.Series([generate_map_of_list_of_map_of_lists() for _ in range(n_rows)], dtype='object')
map_of_structs_col = generate_map_of_structs()
map_of_struct_of_structs_col = generate_map_of_struct_of_structs(n_rows)

# Creating the DataFrame with only the new column
df_nested_list = pd.DataFrame({
    'map': map_col,
    'map_nullable': map_nullable_col,
    'map_of_maps': map_of_maps_col,
    'map_of_lists': map_of_lists_col,
    'map_of_complex_lists': map_of_complex_lists_col,
    'map_of_list_of_map_of_lists': map_of_list_of_map_of_lists_col,
    'map_of_structs': map_of_structs_col,
    'map_of_struct_of_structs': map_of_struct_of_structs_col
})

# Types
map_type = pa.map_(pa.string(), pa.int32())
map_of_maps_type = pa.map_(
    pa.string(),
    pa.map_(
        pa.string(),
        pa.int32()
    )
)
map_of_lists_type = pa.map_(pa.string(), pa.list_(pa.int32()))
map_of_complex_lists_element_type = pa.struct([
    pa.field('int', pa.int32()),
    pa.field('string', pa.string()),
    pa.field('bool', pa.bool_())
])
map_of_complex_lists_type = pa.map_(pa.string(), pa.list_(map_of_complex_lists_element_type))

map_of_list_of_map_of_lists_inner_list_map_type = pa.map_(pa.string(), pa.list_(pa.int32()))
map_of_list_of_map_of_lists_inner_list_type = pa.list_(map_of_list_of_map_of_lists_inner_list_map_type)
map_of_list_of_map_of_lists_type = pa.map_(pa.string(), map_of_list_of_map_of_lists_inner_list_type)

map_of_structs_struct = pa.struct([
    pa.field('int_field', pa.int32()),
    pa.field('string_field', pa.string())
])

# Schema for the map of structs
map_of_structs_type = pa.map_(pa.string(), map_of_structs_struct)

# Schema for the map of struct of structs
map_of_struct_of_structs_struct_struct_struct_type = pa.struct([
    pa.field('int', pa.int32()),
    pa.field('string', pa.string())
])

# Define the schema for the intermediate struct `struct`
map_of_struct_of_structs_struct_struct_type = pa.struct([
    pa.field('nested_struct', map_of_struct_of_structs_struct_struct_struct_type)
])

# Define the schema for the outer struct which includes the 'struct' key
map_of_struct_of_structs_struct_type = pa.struct([
    pa.field('struct', map_of_struct_of_structs_struct_struct_type)
])

# Define the schema for the map `map_of_struct_of_structs`
map_of_struct_of_structs_type = pa.map_(
    pa.field('key', pa.string(), nullable=False),  # Map keys must be non-nullable
    pa.field('value', map_of_struct_of_structs_struct_type)
)

# Define the schema
schema = pa.schema([
    ('map', map_type),
    ('map_nullable', map_type),
    ('map_of_maps', map_of_maps_type),
    ('map_of_lists', map_of_lists_type),
    ('map_of_complex_lists', map_of_complex_lists_type),
    ('map_of_list_of_map_of_lists', map_of_list_of_map_of_lists_type),
    ('map_of_structs', map_of_structs_type),
    ('map_of_struct_of_structs', map_of_struct_of_structs_type),
])

parquet_file = 'output/maps.parquet'
# Create a PyArrow Table
table = pa.Table.from_pandas(df_nested_list, schema=schema)

# Check if the file exists and remove it
if os.path.exists(parquet_file):
    os.remove(parquet_file)

# Write the PyArrow Table to a Parquet file
with pq.ParquetWriter(parquet_file, schema, compression='GZIP') as writer:
    writer.write_table(table)

# Show the first few rows of the DataFrame for verification
print(df_nested_list.head(1))
