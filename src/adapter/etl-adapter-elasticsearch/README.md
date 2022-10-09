# ETL Adapter: Elasticsearch

# Contributing

This repo is **READ ONLY**, in order to contribute to Flow PHP project, please
open PR against [flow](https://github.com/flow-php/flow) monorepo.

Changes merged to monorepo are automatically propagated into sub repositories.

## Installation

```
composer require flow-php/etl-adapter-elasticsearch:1.x@dev
```

## Description

ETL Adapter that provides Loaders and <s>Extractors</s> that works with Elasticsearch.

Following implementation are available: 
- [elasticsearch-php](https://github.com/elastic/elasticsearch-php) 

### Extractor

Elasticsearch extractor will try to extract entire index,
in order to limit results, please use `DataFrame::limit` function combined
with `size` search parameter.

### Transformers

Extractor is turning raw elasticsearch results into ArrayEntries, 
in order to simplify results just to data taken from index please use following extractor.

```
    ->rows(Elasticsearch::hits_to_rows())
```

It will remove everything except data take from `['hits']['hits'][x]['_source']` of each search result. 

## Examples

```php 
<?php

// Load 10k rows into elasticsearch
$this->elasticsearchContext->loadRows(
    new Rows(
        ...\array_map(
            static fn (int $i) : Row => Row::create(
                new Row\Entry\StringEntry('id', \sha1((string) $i)),
                new Row\Entry\IntegerEntry('position', $i),
                new Row\Entry\StringEntry('name', 'id_' . $i),
                new Row\Entry\BooleanEntry('active', false)
            ),
            \range(1, 10_005)
        ),
    ),
    self::INDEX_NAME, new EntryIdFactory('id')
);

// Setup search parameters by adding sort in order to make sure that
// elasticsearch extractor is going to use search_after 
$params = [
    'index' => self::INDEX_NAME,
    'size' => 1001,
    'body'  => [
        'sort' => [
            ['position' => ['order' => 'asc']],
        ],
        'query' => [
            'match_all' => ['boost' => 1.0],
        ],
    ],
];

$results = (new Flow())
    ->extract(Elasticsearch::search($this->elasticsearchContext->clientConfig(), $params))
    ->rows(Elasticsearch::hits_to_rows())
    ->limit($limit = 20)
    ->load(
        Elasticsearch::bulk_index(
            $this->elasticsearchContext->clientConfig(),
            chunk_size: 100,
            index: self::DESTINATION_INDEX,
            id_factory: new EntryIdFactory('id')
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
