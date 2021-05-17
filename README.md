# ETL Adapter: Logger

[![Minimum PHP Version](https://img.shields.io/badge/php-%3E%3D%207.4-8892BF.svg)](https://php.net/)

## Description

ETL Adapter that provides PSR Logger support for ETL.

## Loader - PsrLogger

Load each row into PsrLoggerInterface implementation.

```php
<?php

$logger = new TestLogger();

$loader = new PsrLoggerLoader($logger, 'row log', LogLevel::ERROR);

$loader->load(new Rows(
    Row::create(
        new Row\Entry\IntegerEntry('id', 12345),
        Row\Entry\StringEntry::lowercase('name', 'Norbert')
    )
));

$this->assertTrue($logger->hasErrorRecords());
$this->assertTrue($logger->hasError('row log'));
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