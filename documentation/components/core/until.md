# Until

[DOC_LINK:/documentation/components/core/core.md]

`Until` works almost the same as the [filter](/documentation/components/core/filter.md) function.
The difference: at the first row that matches the condition, it sends a STOP signal to the extractor.

Example:

```php
<?php

data_frame()
    ->read(from_array([
        ['a' => 50, 'b' => 100],
        ['a' => 999, 'b' => 200],
        ['a' => 150, 'b' => 300]
    ]))
    ->until(ref('b')->divide(lit(2))->equals(ref('a')))
    ->write(to_output(false))
    ->run();
```

```text
+----+-----+
|  a |   b |
+----+-----+
| 50 | 100 |
+----+-----+
1 rows
```

Use this feature to stop the extraction at a condition that the data source cannot filter well. An example is a
stop of the extraction at a specific date.