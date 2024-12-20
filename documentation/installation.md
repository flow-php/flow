# Installation

- [⬅️️ Back](introduction.md)

## Installation Methods 

- [Docker](installation/docker.md)
- [PHAR with PHIVE](installation/phive.md)
- [Homebrew](installation/homebrew.md)
- [Quick Start](quick-start.md)

## Composer

If you want to work with JSON/CSV files here are the dependencies you will need to install:

```bash
composer require flow-php/etl flow-php/etl-adapter-csv flow-php/etl-adapter-json
```

Flow is developed as a [monorepo](https://tomasvotruba.com/blog/2019/10/28/all-you-always-wanted-to-know-about-monorepo-but-were-afraid-to-ask/) to reduce maintenance overhead and to make it easier to manage dependencies between components. 

Instead of installing whole repository with all dependencies, it's recommended to install only the components you need.

- [ETL](components/core/core.md)
- Adapters
    - [chartjs](components/adapters/chartjs.md)
    - [csv](components/adapters/csv.md)
    - [doctrine](components/adapters/doctrine.md)
    - [elasticsearch](components/adapters/elasticsearch.md)
    - [google sheet](components/adapters/google-sheet.md)
    - [http](components/adapters/http.md)
    - [json](components/adapters/json.md)
    - [logger](components/adapters/logger.md)
    - [meilisearch](components/adapters/meilisearch.md)
    - [parquet](components/adapters/parquet.md)
    - [text](components/adapters/text.md)
    - [xml](components/adapters/xml.md)
- Libraries
    - [array-dot](components/libs/array-dot.md)
    - [azure-sdk](components/libs/azure-sdk.md)
    - [doctrine-dbal-bulk](components/libs/doctrine-dbal-bulk.md)
    - [dremel](components/libs/dremel.md)
    - [filesystem](components/libs/filesystem.md)
    - [parquet](components/libs/parquet.md)
    - [parquet-viewer](components/libs/parquet-viewer.md)
    - [snappy](components/libs/snappy.md)
- Bridges
    - [filesystem-azure](components/bridges/filesystem-azure-bridge.md)
    - [monolog-http](components/bridges/monolog-http-bridge.md) 

