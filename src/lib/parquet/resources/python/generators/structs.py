import pandas as pd
import random
import os
import pyarrow as pa
import json
import pyarrow.parquet as pq
import sys

# Number of rows to generate
n_rows = 100

# Functions to generate the data
def generate_struct_flat():
    struct_flat_data = []
    for i in range(n_rows):
        string_value = f'string_{i}'
        string_nullable_value = f'string_{i}' if i % 2 == 0 else None
        int_value = i
        int_nullable_value = i if i % 2 == 0 else None
        bool_value = i % 2 == 0
        bool_nullable_value = i % 2 == 0 if i % 2 == 0 else None
        list_of_ints_value = [random.randint(1, 10) for _ in range(3)]
        list_of_strings_value = [f'str_{j}' for j in range(3)]
        map_of_string_int_value = {f'key_{j}': j for j in range(3)}
        map_of_int_int_value = {j: j for j in range(3)}

        struct_flat_element = {
            'string': string_value,
            'string_nullable': string_nullable_value,
            'int': int_value,
            'int_nullable': int_nullable_value,
            'bool': bool_value,
            'bool_nullable': bool_nullable_value,
            'list_of_ints': list_of_ints_value,
            'list_of_strings': list_of_strings_value,
            'map_of_string_int': map_of_string_int_value,
            'map_of_int_int': map_of_int_int_value
        }

        struct_flat_data.append(struct_flat_element)

    return struct_flat_data

def generate_struct_nested():
    struct_nested_data = []
    for i in range(n_rows):
        string_value = f'string_{i}'
        int_value = i
        list_of_ints_value = [random.randint(1, 10) for _ in range(3)]
        map_of_string_int_value = {f'key_{j}': j for j in range(3)}

        struct_element = {
            'int': int_value,
            'list_of_ints': list_of_ints_value,
            'map_of_string_int': map_of_string_int_value
        }

        struct_nested_element = {
            'string': string_value,
            'struct_flat': struct_element
        }

        struct_nested_data.append(struct_nested_element)

    return struct_nested_data

def generate_struct_nested_with_list_of_lists():
    struct_nested_data = []
    for i in range(n_rows):
        string_value = f'string_{i}'
        int_value = i
        # Generating list of lists of integers
        list_of_list_of_ints_value = [[random.randint(1, 10) for _ in range(random.randint(1, 3))] for _ in range(random.randint(1, 3))]

        struct_element = {
            'int': int_value,
            'list_of_list_of_ints': list_of_list_of_ints_value
        }

        struct_nested_element = {
            'string': string_value,
            'struct': struct_element
        }

        struct_nested_data.append(struct_nested_element)

    return struct_nested_data

def generate_struct_nested_with_list_of_maps():
    struct_nested_data = []
    for i in range(n_rows):
        string_value = f'string_{i}'
        int_value = i
        list_of_map_of_string_int_value = [{f'key_{k}': random.randint(1, 10) for k in range(3)} for _ in range(3)]

        struct_element = {
            'int': int_value,
            'list_of_map_of_string_int': list_of_map_of_string_int_value
        }

        struct_nested_element = {
            'string': string_value,
            'struct': struct_element
        }

        struct_nested_data.append(struct_nested_element)

    return struct_nested_data

def generate_struct_nested_with_map_of_list_of_ints():
    struct_nested_data = []
    for i in range(n_rows):
        string_value = f'string_{i}'
        int_value = i
        map_of_int_list_of_string_value = {j: [f'str_{k}' for k in range(3)] for j in range(3)}

        struct_element = {
            'int': int_value,
            'map_of_int_list_of_string': map_of_int_list_of_string_value
        }

        struct_nested_element = {
            'string': string_value,
            'struct': struct_element
        }

        struct_nested_data.append(struct_nested_element)

    return struct_nested_data

def generate_struct_nested_with_map_of_string_map_of_string_string():
    struct_nested_data = []
    for i in range(n_rows):
        string_value = f'string_{i}'
        int_value = i

        map_of_string_map_of_string_string_value = {
            f'outer_key_{j}': {f'inner_key_{k}': f'inner_value_{k}' for k in range(3)}
            for j in range(3)
        }

        struct_element = {
            'int': int_value,
            'map_of_string_map_of_string_string': map_of_string_map_of_string_string_value
        }

        struct_nested_element = {
            'string': string_value,
            'struct': struct_element
        }

        struct_nested_data.append(struct_nested_element)

    return struct_nested_data

def generate_struct_with_list_and_map_of_structs():
    struct_data = []
    for i in range(n_rows):
        string_value = f'string_{i}'

        list_of_structs_value = [{
            'int': j,
            'list': [random.randint(1, 10) for _ in range(3)]
        } for j in range(3)]

        map_of_string_structs_value = {
            f'key_{j}': {
                'int': j,
                'list': [random.randint(1, 10) for _ in range(3)]
            } for j in range(3)
        }

        struct_nested = {
            'int': i,
            'list_of_structs': list_of_structs_value,
            'map_of_string_structs': map_of_string_structs_value
        }

        struct_element = {
            'string': string_value,
            'struct': struct_nested
        }

        struct_data.append(struct_element)

    return struct_data

def generate_struct_deeply_nested():
    struct_deeply_nested_data = []
    for i in range(n_rows):
        json_value = json.dumps({"key": "value"})
        struct_4 = {
            'string': f'string_{i}',
            'json': json_value
        }
        struct_3 = {
            'float': random.uniform(0.0, 1.0),
            'struct_4': struct_4
        }
        struct_2 = {
            'bool': bool(i % 2),
            'struct_3': struct_3
        }
        struct_1 = {
            'string': f'string_{i}',
            'struct_2': struct_2
        }
        struct_0 = {
            'int': i,
            'struct_1': struct_1
        }
        struct_deeply_nested = {
            'struct_0': struct_0
        }
        struct_deeply_nested_data.append(struct_deeply_nested)
    return struct_deeply_nested_data

# Columns
struct_flat_col = generate_struct_flat()
struct_nested_col = generate_struct_nested()
struct_nested_with_list_of_lists_col = generate_struct_nested_with_list_of_lists()
struct_nested_with_list_of_maps_col = generate_struct_nested_with_list_of_maps()
struct_nested_with_map_of_list_of_ints_col = generate_struct_nested_with_map_of_list_of_ints()
struct_nested_with_map_of_string_map_of_string_string_col = generate_struct_nested_with_map_of_string_map_of_string_string()
struct_with_list_and_map_of_structs_col = generate_struct_with_list_and_map_of_structs()
struct_deeply_nested_col = generate_struct_deeply_nested()

# Creating the DataFrame with only the new column
df_nested_list = pd.DataFrame({
    'struct_flat': struct_flat_col,
    'struct_nested': struct_nested_col,
    'struct_nested_with_list_of_lists': struct_nested_with_list_of_lists_col,
    'struct_nested_with_list_of_maps': struct_nested_with_list_of_maps_col,
    'struct_nested_with_map_of_list_of_ints': struct_nested_with_map_of_list_of_ints_col,
    'struct_nested_with_map_of_string_map_of_string_string': struct_nested_with_map_of_string_map_of_string_string_col,
    'struct_with_list_and_map_of_structs': struct_with_list_and_map_of_structs_col,
    'struct_deeply_nested': struct_deeply_nested_col
})

# Types
list_of_ints_type = pa.list_(pa.int32())
list_of_strings_type = pa.list_(pa.string())
map_of_string_int_type = pa.map_(pa.string(), pa.int32())
map_of_int_int_type = pa.map_(pa.int32(), pa.int32())
list_of_list_of_ints_type = pa.list_(list_of_ints_type)
list_of_map_of_string_int_type = pa.list_(pa.map_(pa.string(), pa.int32()))
map_of_int_list_of_string = pa.map_(pa.int32(), pa.list_(pa.string()))
map_of_string_string_type = pa.map_(pa.string(), pa.string())
map_string_map_of_string_string_type = pa.map_(pa.string(), map_of_string_string_type)
struct_with_int_and_list_of_ints_type = pa.struct([
    pa.field('int', pa.int32()),
    pa.field('list', pa.list_(pa.int32()))
])

#### struct_flat
struct_flat_type = pa.struct([
    pa.field('string', pa.string()),
    pa.field('string_nullable', pa.string()),
    pa.field('int', pa.int32()),
    pa.field('int_nullable', pa.int32()),
    pa.field('bool', pa.bool_()),
    pa.field('bool_nullable', pa.bool_()),
    pa.field('list_of_ints', list_of_ints_type),
    pa.field('list_of_strings', list_of_strings_type),
    pa.field('map_of_string_int', map_of_string_int_type),
    pa.field('map_of_int_int', map_of_int_int_type),
])

#### Struct Nested
list_of_ints_type = pa.list_(pa.int32())
map_of_string_int_type = pa.map_(pa.string(), pa.int32())

struct_nested_struct_flat_type = pa.struct([
    pa.field('int', pa.int32()),
    pa.field('list_of_ints', list_of_ints_type),
    pa.field('map_of_string_int', map_of_string_int_type)
])

struct_nested_type = pa.struct([
    pa.field('string', pa.string()),
    pa.field('struct_flat', struct_nested_struct_flat_type)
])

#### Struct with a list of lists of integers
struct_nested_with_list_of_lists_struct_type = pa.struct([
    pa.field('int', pa.int32()),
    pa.field('list_of_list_of_ints', list_of_list_of_ints_type)
])

struct_nested_with_list_of_lists_type = pa.struct([
    pa.field('string', pa.string()),
    pa.field('struct', struct_nested_with_list_of_lists_struct_type)
])

#### Struct with list of maps of string to int
struct_type = pa.struct([
    pa.field('int', pa.int32()),
    pa.field('list_of_map_of_string_int', list_of_map_of_string_int_type)
])

struct_nested_with_list_of_maps_type = pa.struct([
    pa.field('string', pa.string()),
    pa.field('struct', struct_type)
])
#### Struct with map of int to list of string
struct_nested_with_map_of_list_of_ints_struct_type = pa.struct([
    pa.field('int', pa.int32()),
    pa.field('map_of_int_list_of_string', map_of_int_list_of_string)
])
struct_nested_with_map_of_list_of_ints_type = pa.struct([
    pa.field('string', pa.string()),
    pa.field('struct', struct_nested_with_map_of_list_of_ints_struct_type)
])
#### Struct with map of string to map of string to string
struct_nested_with_map_of_string_map_of_string_string_struct_type = pa.struct([
    pa.field('int', pa.int32()),
    pa.field('map_of_string_map_of_string_string', map_string_map_of_string_string_type)
])
struct_nested_with_map_of_string_map_of_string_string_type = pa.struct([
    pa.field('string', pa.string()),
    pa.field('struct', struct_nested_with_map_of_string_map_of_string_string_struct_type)
])
#### Struct with list and map of structs
struct_with_list_and_map_of_structs_list_of_structs_type = pa.list_(struct_with_int_and_list_of_ints_type)
struct_with_list_and_map_of_structs_map_of_string_structs_type = pa.map_(pa.string(), struct_with_int_and_list_of_ints_type)

struct_with_list_and_map_of_structs_struct_type = pa.struct([
    pa.field('int', pa.int32()),
    pa.field('list_of_structs', struct_with_list_and_map_of_structs_list_of_structs_type),
    pa.field('map_of_string_structs', struct_with_list_and_map_of_structs_map_of_string_structs_type)
])

struct_with_list_and_map_of_structs_type = pa.struct([
    pa.field('string', pa.string()),
    pa.field('struct', struct_with_list_and_map_of_structs_struct_type)
])
#### Struct deeply nested
struct_4_type = pa.struct([
    pa.field('string', pa.string()),
    pa.field('json', pa.string())
])
struct_3_type = pa.struct([
    pa.field('float', pa.float32()),
    pa.field('struct_4', struct_4_type)
])
struct_2_type = pa.struct([
    pa.field('bool', pa.bool_()),
    pa.field('struct_3', struct_3_type)
])
struct_1_type = pa.struct([
    pa.field('string', pa.string()),
    pa.field('struct_2', struct_2_type)
])
struct_0_type = pa.struct([
    pa.field('int', pa.int32()),
    pa.field('struct_1', struct_1_type)
])
struct_deeply_nested_type = pa.struct([
    pa.field('struct_0', struct_0_type)
])

# Define the schema
schema = pa.schema([
    ('struct_flat', struct_flat_type),
    ('struct_nested', struct_nested_type),
    ('struct_nested_with_list_of_lists', struct_nested_with_list_of_lists_type),
    ('struct_nested_with_list_of_maps', struct_nested_with_list_of_maps_type),
    ('struct_nested_with_map_of_list_of_ints', struct_nested_with_map_of_list_of_ints_type),
    ('struct_nested_with_map_of_string_map_of_string_string', struct_nested_with_map_of_string_map_of_string_string_type),
    ('struct_with_list_and_map_of_structs', struct_with_list_and_map_of_structs_type),
    ('struct_deeply_nested', struct_deeply_nested_type),
])

parquet_file = 'output/structs.parquet'

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
