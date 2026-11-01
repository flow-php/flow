# Display

[DOC_LINK:/documentation/components/core/core.md]

Developers often display or print a data frame during development.
It is the easiest way to debug a data frame.
By default, it takes the selected number of rows (20 by default).

## Example

`display()` returns a string. `write(to_output())` prints one.

```php
<?php

use function Flow\ETL\DSL\{data_frame, from_array, int_schema, ref, schema, str_schema};

echo data_frame()
    ->read(from_array([
        ['id' => 1, 'name' => 'norbert'],
        ['id' => 2, 'name' => 'jane'],
        ['id' => 3, 'name' => 'john'],
    ], schema(int_schema('id'), str_schema('name'))))
    ->withColumn('name', ref('name')->upper())
    ->collect()
    ->display($limit = 2);
```

```text
+----+---------+
| id |    name |
+----+---------+
|  1 | NORBERT |
|  2 |    JANE |
+----+---------+
2 rows
```

You can also display the data frame with the `DataFrame::write` function:

```php
<?php

use function Flow\ETL\DSL\{data_frame, from_array, int_schema, ref, schema, str_schema, to_output};

data_frame()
    ->read(from_array([
        ['id' => 1, 'name' => 'norbert'],
        ['id' => 2, 'name' => 'jane'],
    ], schema(int_schema('id'), str_schema('name'))))
    ->withColumn('name', ref('name')->upper())
    ->collect()
    ->write(to_output(truncate: false))
    ->run();
```

