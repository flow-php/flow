# Limit

- [⬅️️ Back](core.md)

Sometimes you might just want to process only few first rows, maybe for debugging purpose or you don't want to go above certain number of rows.

In this example, Pipeline will take only 5 rows from Extractor passing them through all transformers.

## Example 

```php
<?php 

data_frame()
    ->read(from_())
    ->limit(5)
    ->write(to_())
    ->run();
```

It's important to remember that some transformers might actually change the original number of rows extracted
from the source, like, for example, when you need to expand an array of elements in single row, into new rows.

In that case, Flow will detect the expansion, and it will cut rows accordingly to the total limit, however,
it will happen only after expanding transformation. It's done this way because there is no way to predict
the result of the expansion.