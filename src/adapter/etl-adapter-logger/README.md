# ETL Adapter: Logger

ETL Adapter that provides PSR Logger support for ETL.

## Installation

```
composer require flow-php/etl-adapter-logger:1.x@dev
```

## Loader - PsrLogger

Load each row into PsrLoggerInterface implementation. To get `TestLogger` mock class first run:

```
composer require fig/log-test
```

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