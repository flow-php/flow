# ETL Adapter: JSON

[![Minimum PHP Version](https://img.shields.io/badge/php-%3E%3D%207.4-8892BF.svg)](https://php.net/)

## Description

ETL Adapter that provides memory safe JSON support for ETL.

Following implementation are available: 
- [JSON Machine](https://github.com/halaxa/json-machine) 

## Entry - JsonEntry

```php 
<?php

use Flow\ETL\Row\Entry\JsonEntry;

$jsonEntry = new JsonEntry('empty', []);
$jsonObjectEntry = JsonEntry::object('empty', []);

$this->assertEquals('[]', $jsonEntry->value());
$this->assertEquals('{}', $jsonObjectEntry->value());
```

## Extractor - JSONMachineExtractor

```php
<?php

use Flow\ETL\Adapter\JSON\JSONMachineExtractor;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use JsonMachine\JsonMachine;

$extractor = new JSONMachineExtractor(
    JsonMachine::fromFile(__DIR__ . '/../Fixtures/timezones.json'), 
    5
);

/** @var Rows $rows */
foreach ($extractor->extract() as $rows) {
    // Do something with Row 
}
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
