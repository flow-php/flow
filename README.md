# Flow PHP 

Data manipulation library

```
composer require flow-php/flow
```

* [extractors](src/Flow/ETL/DSL/extractors.php)
* [transformers](src/Flow/ETL/DSL/transformers.php)
* [loaders](src/Flow/ETL/DSL/loaders.php)

Examples:

```php
<?php

use function Flow\ETL\DSL\Transformer\{columnNameConvert, expand, filter, keepColumns, castTo, unpack};
use function Flow\ETL\DSL\Extractor\{extractArray, extractCSV, extractJSON};
use function Flow\ETL\DSL\Loader\{debug, debugEntries, toCSV};

$data = [
    ['id' => 1, 'name' => 'Norbert', 'status' => 'premium', 'updatedAt' => '2020-01-01 00:00:00', 'properties' => [1, 2, 3]],
    ['id' => 2, 'name' => 'John', 'status' => 'premium', 'updatedAt' => '2020-01-02 00:00:00', 'properties' => [4, 5]],
    ['id' => 3, 'name' => 'Steve', 'status' => 'free', 'updatedAt' => '2020-01-03 00:00:00', 'properties' => [6]],
];

extractArray($data)
    ->transform(filter('status', fn(string $status) => $status === 'premium'))
    ->transform(columnNameConvert('snake'))
    ->transform(keepColumns('id', 'name', 'updated_at', 'properties'))
    ->transform(castTo('datetime', ['updated_at'], 'Y-m-d H:i:s', 'UTC'))
    ->transform(castTo('json', ['properties']))
    ->load(toCSV(__DIR__ . '/premium_users.csv'))
    ->run();
```