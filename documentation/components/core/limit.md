# Limit

[DOC_LINK:/documentation/components/core/core.md]

Sometimes you might just want to process only few first rows, maybe for debugging purpose or you don't want to go above certain number of rows.

In this example, Pipeline will take only 5 rows from Extractor passing them through all transformers.

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

It's important to remember that some transformers might actually change the original number of rows extracted
from the source, like, for example, when you need to expand an array of elements in single row, into new rows.

In that case, Flow will detect the expansion, and it will cut rows accordingly to the total limit, however,
it will happen only after expanding transformation. It's done this way because there is no way to predict
the result of the expansion.
## Limit Push-Down

When nothing between `read()` and `limit()` can change the number of rows, the optimizer hands the limit to the
extractor, so a source that supports it reads only what is needed (`offset()` in between adds the skipped rows). A
frame read with `from_data_frame()` passes the limit on to its own source the same way.

```php
<?php

use function Flow\ETL\Adapter\CSV\from_csv;
use function Flow\ETL\DSL\{data_frame, from_data_frame};

$orders = data_frame()->read(from_csv(__DIR__ . '/orders.csv'))->select('id', 'total');

data_frame()
    ->read(from_data_frame($orders))
    ->offset(10)
    ->limit(5)
    ->fetch(); // the CSV extractor is asked for 15 rows
```
