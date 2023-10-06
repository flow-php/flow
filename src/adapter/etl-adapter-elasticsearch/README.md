# ETL Adapter: Elasticsearch

Flow PHP's Adapter Elasticsearch is a sophisticated library devised to facilitate seamless interaction with
Elasticsearch within your ETL (Extract, Transform, Load) workflows. This adapter is essential for developers aiming to
effortlessly extract from or load data into Elasticsearch, ensuring a streamlined and reliable data transformation
experience. By employing the Adapter Elasticsearch library, developers can access a robust suite of features tailored
for precise interaction with Elasticsearch, simplifying complex data transformations and boosting data processing
efficiency. The Adapter Elasticsearch library encapsulates a comprehensive range of functionalities, offering a
streamlined API for managing Elasticsearch tasks, which is vital in contemporary data processing and transformation
scenarios. This library manifests Flow PHP's commitment to providing versatile and efficient data processing solutions,
making it an excellent choice for developers dealing with Elasticsearch in large-scale and data-intensive environments.
With Flow PHP's Adapter Elasticsearch, managing Elasticsearch data within your ETL workflows becomes a more refined and
efficient endeavor, harmoniously aligning with the robust and adaptable framework of the Flow PHP ecosystem.

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
