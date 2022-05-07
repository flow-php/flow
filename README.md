# ETL Adapter: CSV

[![Minimum PHP Version](https://img.shields.io/badge/php-~8.1-8892BF.svg)](https://php.net/)

## Description

ETL Adapter that provides Loaders and Extractors that works with CSV files.

Following implementation are available: 
- [League CSV](https://csv.thephpleague.com/) 

## Installation 

``` 
composer require flow-php/etl-adapter-csv
composer require league/csv
```

> League CSV adapter is not explicitly required, you need to make sure it is available in your composer.json file.

## Extractor - League CSVExtractor

```php
<?php

use Flow\ETL\DSL\CSV;
use Flow\ETL\Adapter\CSV\League\CSVExtractor;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use League\Csv\Reader;

$extractor = CSV::from_file(
    __DIR__ . '/../Fixtures/annual-enterprise-survey-2019-financial-year-provisional-csv.csv',
    $rowsInBatch = 5,
    $offsetHeader = 0
);

/** @var Rows $rows */
foreach ($extractor->extract() as $rows) {
    // Do something with Row 
}
```

## Loader - League CSVLoader

```php 
<?php

use Flow\ETL\DSL\CSV;
use Flow\ETL\Adapter\CSV\League\CSVLoader;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use League\Csv\Writer;

$loader = new CSV::to_file(
    $path = \sys_get_temp_dir() . '/' . \uniqid('flow_php_etl_csv_loader', true) . '.csv'
);

$loader->load(new Rows(
    Row::create(new Row\Entry\ArrayEntry('row', ['id', 'name'])),
    Row::create(new Row\Entry\ArrayEntry('row', [1, 'Norbert'])),
));
$loader->load(new Rows(
    Row::create(new Row\Entry\ArrayEntry('row', [2, 'Tomek'])),
    Row::create(new Row\Entry\ArrayEntry('row', [3, 'Dawid'])),
));
```

> If `CSV::to_file` will be used in async pipeline due to concurrency issues it will be turned into
> `CSV::to_directory`. Each process will write random file in the directory.

## Development

In order to install dependencies please, launch following commands:

```bash
composer install
```

## Run Tests

In order to execute full test suite, please launch following command:

```bash
composer build
```

It's recommended to use [pcov](https://pecl.php.net/package/pcov) for code coverage however you can also use
xdebug by setting `XDEBUG_MODE=coverage` env variable.
