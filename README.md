# ETL Adapter: CSV

[![Minimum PHP Version](https://img.shields.io/badge/php-%3E%3D%207.4-8892BF.svg)](https://php.net/)

## Description

ETL Adapter that provides Loaders and Extractors that works with CSV files.

Following implementation are available: 
- [League CSV](https://csv.thephpleague.com/) 


## Extractor - League CSVExtractor

```php
<?php

use Flow\ETL\Adapter\CSV\League\CSVExtractor;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use League\Csv\Reader;

$extractor = new CSVExtractor(
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

use Flow\ETL\Adapter\CSV\League\CSVLoader;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use League\Csv\Writer;

$loader = new CSVLoader(
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
