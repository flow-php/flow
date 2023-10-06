# ETL Adapter: MeiliSearch

Flow PHP's Adapter MeiliSearch is a refined library designed to seamlessly integrate MeiliSearch capabilities within
your ETL (Extract, Transform, Load) workflows. This adapter is essential for developers seeking to effortlessly interact
with MeiliSearch, a powerful, fast, and open-source search engine, thereby enhancing the search and indexing
functionalities of their data transformation processes. By leveraging the Adapter MeiliSearch library, developers can
tap into a robust suite of features engineered for precise interaction with MeiliSearch, simplifying complex search and
indexing operations while enhancing overall data processing efficiency. The Adapter MeiliSearch library encapsulates a
rich set of functionalities, offering a streamlined API for managing search and indexing tasks, which is crucial in
contemporary data processing and transformation endeavors. This library epitomizes Flow PHP's commitment to delivering
versatile and efficient data processing solutions, making it an excellent choice for developers dealing with search and
indexing operations in large-scale and data-intensive environments. With Flow PHP's Adapter MeiliSearch, navigating
search and indexing tasks within your ETL workflows becomes a more streamlined and efficient endeavor, harmoniously
aligning with the robust and adaptable framework of the Flow PHP ecosystem.

## Installation

```
composer require flow-php/etl-adapter-meilisearch:1.x@dev
```

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

## Contributing

This repo is **READ ONLY**, in order to contribute to Flow PHP project, please
open PR against [flow](https://github.com/flow-php/flow) monorepo.

Changes merged to monorepo are automatically propagated into sub repositories.

