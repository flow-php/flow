# ETL Adapter: Text

# Contributing

This repo is **READ ONLY**, in order to contribute to Flow PHP project, please
open PR against [flow](https://github.com/flow-php/flow) monorepo.

Changes merged to monorepo are automatically propagated into sub repositories.

## Description

ETL Adapter that provides Loaders and Extractors that works with Text files.
It does not require any external dependencies, it's working on internal PHP functions.

## Installation 

``` 
composer require flow-php/etl-adapter-text
```

## Extractor 

```php
<?php

use Flow\ETL\DSL\Text;
use Flow\ETL\Flow;

$rows = (new Flow())
    ->read(Text::from(new LocalFile($path)))
    ->fetch();
```

## Loader 

> :warning: Heads up, TextLoader expects rows to have single entry in order to properly write them into file.

```php 
<?php

use Flow\ETL\DSL\Text;
use Flow\ETL\Row;
use Flow\ETL\Rows;

(new Flow())
    ->process(
        new Rows(
            Row::create(new Row\Entry\StringEntry('name', 'Norbert')),
            Row::create(new Row\Entry\StringEntry('name', 'Tomek')),
            Row::create(new Row\Entry\StringEntry('name', 'Dawid')),
        )
    )
    ->load(Text::to($path))
    ->run();
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
