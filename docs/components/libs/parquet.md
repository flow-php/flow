# Parquet

- [⬅️️ Back](../../introduction.md)

## Installation

```
composer require flow-php/parquet:1.x@dev
```

## Usage

Reading whole file:
```php
<?php

use Flow\Parquet\Reader;

$reader = new Reader();
$parquet = $reader->read(__DIR__ . '/orders_spark.parquet');

foreach ($parquet->values() as $row) {
    var_dump($row);
}
```

Reading specific number of rows:
```php
<?php

use Flow\Parquet\Reader;

$reader = new Reader();
$parquet = $reader->read(__DIR__ . '/orders_spark.parquet');

foreach ($parquet->values(limit: 100) as $row) {
    var_dump($row);
}
```

Reading specific columns:
```php
<?php

use Flow\Parquet\Reader;

$reader = new Reader();
$parquet = $reader->read(__DIR__ . '/orders_spark.parquet');

foreach ($parquet->values(["order_id"]) as $row) {
    var_dump($row);
}
```