# Limit

[DOC_LINK:/documentation/components/core/core.md]

Sometimes you want to process only the first few rows, for example to debug, or to stay below a specific number of rows.

In this example, the Pipeline takes only 5 rows from the Extractor and sends them through all transformers.

## Example 

```php
<?php

use function Flow\ETL\DSL\{data_frame, from_sequence_number, to_output};

data_frame()
    ->read(from_sequence_number('id', 1, 100))
    ->limit(5)
    ->collect()
    ->write(to_output(truncate: false))
    ->run();
```

```text
+----+
| id |
+----+
|  1 |
|  2 |
|  3 |
|  4 |
|  5 |
+----+
5 rows
```

Some transformers can change the original number of rows from the source. An example is the expansion of an array
of elements in one row into new rows.

In that case, Flow detects the expansion and cuts the rows to the total limit. But this happens only after the
expanding transformation, because the result of the expansion cannot be predicted.
## Limit Push-Down

Sometimes nothing between `read()` and `limit()` can change the number of rows. The optimizer then gives the limit
to the extractor. A source that supports it reads only the rows that it needs. An `offset()` between them adds the
skipped rows.

```php
<?php

use function Flow\ETL\Adapter\CSV\from_csv;
use function Flow\ETL\DSL\data_frame;

data_frame()
    ->read(from_csv(__DIR__ . '/orders.csv'))
    ->select('id', 'total')
    ->offset(10)
    ->limit(5)
    ->fetch(); // the CSV extractor skips 10 rows and stops after 5
```
