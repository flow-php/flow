# ETL Adapter: Meilisearch

# Contributing

This repo is **READ ONLY**, in order to contribute to Flow PHP project, please
open PR against [flow](https://github.com/flow-php/flow) monorepo.

Changes merged to monorepo are automatically propagated into sub repositories.

## Installation

```
composer require flow-php/etl-adapter-meilisearch:1.x@dev
```

## Description

ETL Adapter that provides Loaders and <s>Extractors</s> that works with Meilisearch.

Following implementation are available: 
- [meilisearch-php](https://github.com/meilisearch/meilisearch-php) 

### Extractor

Meilisearch extractor will try to extract entire index, but Meilisearch by [default limits the window
to 1k of hits](https://www.meilisearch.com/docs/reference/api/settings#pagination), this can be changed by update
of index settings:
```php
$this->meilisearchContext->client()->index(self::INDEX_NAME)->updateSettings(['pagination' => ['maxTotalHits' => 10000]]);
```

### Transformers

Extractor is turning raw meilisearch results into ArrayEntries, 
in order to simplify results just to data taken from index please use following extractor.

```
    ->rows(Meilisearch::hits_to_rows())
```

It will remove everything except data take from `['hits']` of each search result. 

## Examples

```php 
<?php

// Load 1k rows into meilisearch
$this->meiliseachContext->loadRows(
    new Rows(
        ...\array_map(
            static fn (int $i) : Row => Row::create(
                new Row\Entry\StringEntry('id', \sha1((string) $i)),
                new Row\Entry\IntegerEntry('position', $i),
                new Row\Entry\StringEntry('name', 'id_' . $i),
                new Row\Entry\BooleanEntry('active', false)
            ),
            \range(1, 1_000)
        ),
    ),
    self::INDEX_NAME
);
 
$params = [
    'q' => '',
];

$results = (new Flow())
    ->extract(Meilisearch::search($this->meilisearchContext->clientConfig(), $params, self::INDEX_NAME))
    ->rows(Meilisearch::hits_to_rows())
    ->limit($limit = 20)
    ->load(
        Meilisearch::bulk_index(
            $this->meiliseachContext->clientConfig(),
            self::DESTINATION_INDEX,
        )
    )
    ->fetch();

$this->assertCount($limit, $results);

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
