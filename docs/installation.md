# Installation

- [⬅️️ Back](introduction.md)

This repository is a [monorepo](https://tomasvotruba.com/blog/2019/10/28/all-you-always-wanted-to-know-about-monorepo-but-were-afraid-to-ask/).
Please check the below packages and select only those that you are going to use,
this will reduce the number of unnecessary dependencies in your project (less maintenance).

- [ETL](components/core/core.md)
- Adapters
    - [avro](components/adapters/avro.md)
    - [chartjs](components/adapters/chartjs.md)
    - [csv](components/adapters/csv.md)
    - [doctrine](components/adapters/doctrine.md)
    - [elasticsearch](components/adapters/elasticsearch.md)
    - [filesystem](components/adapters/filesystem.md)
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
    - [doctrine-dbal-bulk](components/libs/doctrine-dbal-bulk.md)
    - [dremel](components/libs/dremel.md)
    - [parquet](components/libs/parquet.md)
    - [parquet-viewer](components/libs/parquet-viewer.md)
    - [snappy](components/libs/snappy.md)

For example, if you want to work with JSON/CSV files here are the dependencies you will need to install:

- ➡️ Composer
```bash
composer require flow-php/etl flow-php/etl-adapter-csv flow-php/etl-adapter-json
```

- [➡️ Docker](installation/docker.md)
- [➡️ PHAR with PHIVE](installation/phive.md)
- [➡️ Homebrew](installation/homebrew.md)
- [➡️ Quick Start](quick-start.md)
