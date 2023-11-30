# Until

- [⬅️️ Back](core.md)

`Until` works in a very similar way to [filter](filter.md) function. 
The difference is that it will send STOP signal to extractor when the condition is met.

Example:

```php
<?php

data_frame()
    ->read(from_array([
        ['a' => 100, 'b' => 100],
        ['a' => 100, 'b' => 200]
    ]))
    ->until(ref('b')->divide(lit(2))->equals(lit('a')))
    ->write(to_output(false))
    ->run();
```

This feature is useful when you want to stop the extraction process when a certain condition is met but the data source
does not provide a good way to filter the data. For example, when you want to stop the extraction process when you reach
a certain date. 