# Filter

- [⬅️️ Back](core.md)

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

- [➡️ Until](until.md)
