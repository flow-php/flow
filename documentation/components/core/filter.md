# Filter

- [⬅️️ Back](/documentation/components/core/core.md)

To filter rows from the data frame you can use `DataFrame::filter` function.
Filter function accepts only one argument which is a `ScalarFunction` that returns `bool` value.

Example:

```php
<?php

data_frame()
    ->read(from_array([
        ['a' => 100, 'b' => 100],
        ['a' => 100, 'b' => 200]
    ]))
    ->filter(ref('b')->divide(lit(2))->equals(lit('a')))
    ->write(to_output(false))
    ->run();
```

## Complex Row-level Filtering

For advanced filtering that requires custom business logic, you can use callback functions:

```php
<?php

use Flow\ETL\Row;

data_frame()
    ->read($transactionExtractor)
    ->filter(function(Row $row): bool {
        $amount = $row->get('amount')->value();
        $type = $row->get('type')->value();
        $date = $row->get('date')->value();
        
        // Complex business logic
        return $amount > 1000 
            && $type === 'purchase' 
            && $date > new DateTime('-30 days');
    })
    ->write($highValueTransactionLoader)
    ->run();
```

> **Performance Note**: Callback-based filtering cannot be optimized by the engine and should be used sparingly. When possible, prefer built-in scalar functions for better performance.

- [➡️ Until](until.md)
