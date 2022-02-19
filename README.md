# ETL Adapter: JSON

[![Minimum PHP Version](https://img.shields.io/badge/php-%3E%3D%207.4-8892BF.svg)](https://php.net/)

## Description

ETL Adapter that provides memory safe JSON support for ETL.

Following implementation are available: 
- [JSON Machine](https://github.com/halaxa/json-machine) 

## Extractor - JSONMachine - JsonExtractor

```php
<?php

use Flow\ETL\Adapter\JSON\JSONMachine\JsonExtractor;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use JsonMachine\JsonMachine;

$extractor = new JsonExtractor(
    __DIR__ . '/../Fixtures/timezones.json', 
    5
);

/** @var Rows $rows */
foreach ($extractor->extract() as $rows) {
    // Do something with Row 
}
```

## Loader - JsonLoader

```php
<?php

$loader = new JsonLoader(\sys_get_temp_dir() . '/file.json');
$loader->load(new Rows(...));
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
