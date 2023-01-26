# ETL Adapter: Google Sheet

## Description

ETL Adapter that provides Extractor that works with Google sheets.

# Contributing

This repo is **READ ONLY**, in order to contribute to Flow PHP project, please
open PR against [flow](https://github.com/flow-php/flow) monorepo.

Changes merged to monorepo are automatically propagated into sub repositories.


## Installation 

``` 
composer require flow-php/etl-adapter-google-sheet:1.x@dev
```

## Extractor

```php
<?php

use Flow\ETL\Adapter\GoogleSheet\GoogleSheetRange;
use Flow\ETL\DSL\GoogleSheet;
use Flow\ETL\Flow;

$client = new Google\Client();
//...
        
$rows = (new Flow())
    ->read(GoogleSheet::from('spread-sheet-id', $client, GoogleSheetRange::create('Sheet', 'A', 1, 'B', 2)))
    ->fetch();
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
