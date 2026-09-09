# Display

[DOC_LINK:/documentation/components/core/core.md]

Displaying/printing data frame is a common operation during development. 
It's the easiest way to debug data frame.
By default, it will grab the selected number of rows (20 by default)

## Example

`display()` returns a string; `write(to_output())` prints one.

```php
<?php

use function Flow\ETL\DSL\{data_frame, from_array, int_schema, ref, schema, str_schema};

echo data_frame()
    ->read(from_array([
        ['id' => 1, 'name' => 'norbert'],
        ['id' => 2, 'name' => 'jane'],
        ['id' => 3, 'name' => 'john'],
    ], schema(int_schema('id'), str_schema('name'))))
    ->withEntry('name', ref('name')->upper())
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

Alternatively you can use `DataFrame::write` function to display data frame:

```php
<?php

use function Flow\ETL\DSL\{data_frame, from_array, int_schema, ref, schema, str_schema, to_output};

data_frame()
    ->read(from_array([
        ['id' => 1, 'name' => 'norbert'],
        ['id' => 2, 'name' => 'jane'],
    ], schema(int_schema('id'), str_schema('name'))))
    ->withEntry('name', ref('name')->upper())
    ->collect()
    ->write(to_output(truncate: false))
    ->run();
```

