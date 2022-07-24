# ETL Adapter: Elasticsearch

# Contributing

This repo is **READ ONLY**, in order to contribute to Flow PHP project, please
open PR against [flow](https://github.com/flow-php/flow) monorepo.

Changes merged to monorepo are automatically propagated into sub repositories.


## Description

ETL Adapter that provides Loaders and <s>Extractors</s> that works with Elasticsearch.

Following implementation are available: 
- [elasticsearch-php](https://github.com/elastic/elasticsearch-php) 


## Loader - LeagueCSVLoader

```php 
<?php

use Flow\ETL\Adapter\Elasticsearch\ElasticsearchPHP\ElasticsearchLoader;
use Flow\ETL\Adapter\Elasticsearch\EntryIdFactory\Sha1IdFactory;
use Flow\ETL\Row;
use Flow\ETL\Rows;

$loader = new ElasticsearchLoader(
    ['hosts' => ['localhost:9200']], 
    $bulkSize = 2, 
    self::INDEX_NAME, 
    new Sha1IdFactory('id'), 
    $params = ['refresh' => true]
);

$loader->load(new Rows(
    Row::create(
        new Row\Entry\IntegerEntry('id', 1),
        new Row\Entry\StringEntry('name', 'Łukasz')
    ),
    Row::create(
        new Row\Entry\IntegerEntry('id', 2),
        new Row\Entry\StringEntry('name', 'Norbert')
    ),
    Row::create(
        new Row\Entry\IntegerEntry('id', 3),
        new Row\Entry\StringEntry('name', 'Dawid')
    ),
    Row::create(
        new Row\Entry\IntegerEntry('id', 4),
        new Row\Entry\StringEntry('name', 'Tomek')
    ),
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
cp docker-compose.yaml.dist docker-compose.yaml
docker-compose up
composer build
```

It's recommended to use [pcov](https://pecl.php.net/package/pcov) for code coverage however you can also use
xdebug by setting `XDEBUG_MODE=coverage` env variable.
