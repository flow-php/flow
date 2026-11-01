---
package: flow-php/etl-adapter-seal
---

# ETL Adapter: Seal

[PACKAGE_NAV]

[TOC]

This is the Flow PHP Adapter for SEAL. [SEAL](https://php-cmsig.github.io/search/) is a PHP Search Engine Abstraction Layer with
one API for many search engines (Algolia, Elasticsearch, Meilisearch, OpenSearch, Solr, Typesense, RediSearch, Loupe).
With this adapter, your Flow PHP ETL pipelines can load data into each search backend that SEAL supports. The
pipelines do not depend on a specific search engine implementation.

## Installation

For the installation instructions, read the [installation page](/documentation/installation/packages/etl-adapter-seal.md).

Also install the SEAL engine adapter for your search backend, for example:

```
composer require flow-php/etl-adapter-seal cmsig/seal-elasticsearch-adapter
```

| Backend       | Package                            |
|---------------|------------------------------------|
| Algolia       | `cmsig/seal-algolia-adapter`       |
| Elasticsearch | `cmsig/seal-elasticsearch-adapter` |
| Loupe         | `cmsig/seal-loupe-adapter`         |
| Meilisearch   | `cmsig/seal-meilisearch-adapter`   |
| Memory        | `cmsig/seal-memory-adapter`        |
| OpenSearch    | `cmsig/seal-opensearch-adapter`    |
| RediSearch    | `cmsig/seal-redisearch-adapter`    |
| Solr          | `cmsig/seal-solr-adapter`          |
| Typesense     | `cmsig/seal-typesense-adapter`     |

## Description

The adapter works on a `CmsIg\Seal\EngineInterface`. You build it yourself from the SEAL adapter of your choice and
a SEAL schema. The Doctrine and PostgreSQL adapters work in the same way: they accept a pre-configured
connection/client. It also works with the [SEAL Symfony bundle](https://php-cmsig.github.io/search/integrations/symfony.html),
which can inject a fully configured engine.

- Loader: `to_seal_upsert()` / `to_seal_delete()` write or remove documents in a search index in bulks.
- Schema Conversion: `to_seal_schema()` / `seal_schema_to_flow()` convert between Flow and SEAL schemas.

## Building an Engine

```php
use CmsIg\Seal\Adapter\Elasticsearch\ElasticsearchAdapter;
use CmsIg\Seal\Engine;

use function Flow\ETL\Adapter\Seal\to_seal_schema;
use function Flow\ETL\DSL\{int_schema, schema, str_schema};

$sealSchema = to_seal_schema(
    schema(
        str_schema('id'),
        str_schema('name'),
        int_schema('age'),
    ),
    index_name: 'users',
    identifier: 'id',
);

$engine = new Engine(new ElasticsearchAdapter($client), $sealSchema);
$engine->createIndex('users');
```

## Loader

### Upserting Documents

`to_seal_upsert()` saves documents into the index. Search engines upsert by document identifier. A document with an
existing identifier replaces the old document.

```php
use function Flow\ETL\Adapter\Seal\to_seal_upsert;
use function Flow\ETL\Adapter\CSV\from_csv;
use function Flow\ETL\DSL\data_frame;

data_frame()
    ->read(from_csv(__DIR__ . '/users.csv'))
    ->write(to_seal_upsert($engine, 'users'))
    ->run();
```

### Bulk Size

By default, documents are written in bulks of 100. To change this, use `withBulkSize()`:

```php
to_seal_upsert($engine, 'users')->withBulkSize(500);
```

### Deleting Documents

`to_seal_delete()` removes documents by their identifiers, which it reads from the `id` column. `withIdentifierColumn()`
names a different column.

```php
use function Flow\ETL\Adapter\Seal\to_seal_delete;
use function Flow\ETL\DSL\{data_frame, from_array};

data_frame()
    ->read(from_array([['id' => '1'], ['id' => '2']]))
    ->write(to_seal_delete($engine, 'users'))
    ->run();
```

```php
to_seal_delete($engine, 'products')->withIdentifierColumn('sku');
```

### Data Normalization

Each row becomes one document:

| Flow Type                                    | Document Value                                  |
|----------------------------------------------|-------------------------------------------------|
| `string`, `int`, `float`, `bool`             | unchanged                                       |
| `uuid`                                       | string representation                           |
| `datetime`                                   | string, `DateTimeInterface::ATOM` by default    |
| `date`                                       | string, `Y-m-d` by default                      |
| `enum`                                       | case name                                       |
| `time`                                       | microseconds                                    |
| `xml`, `xml_element`, `html`, `html_element` | markup text, for example `<a b="1"><c></c></a>` |
| `json`                                       | decoded array                                   |
| `list`, `map`, `structure`                   | array (nested values normalized recursively)    |

## Schema Conversion

`to_seal_schema()` converts a Flow schema into a SEAL schema for one index. SEAL needs exactly one identifier field
for each index. Name it with the `identifier` argument, or mark a definition with `SealMetadata::identifier()`.

```php
use Flow\ETL\Adapter\Seal\Values\Schema\SealMetadata;

use function Flow\ETL\Adapter\Seal\to_seal_schema;
use function Flow\ETL\DSL\{datetime_schema, float_schema, int_schema, schema, str_schema};

$sealSchema = to_seal_schema(
    schema(
        str_schema('id'),
        str_schema('title', metadata: SealMetadata::filterable()->merge(SealMetadata::sortable())),
        float_schema('price'),
        datetime_schema('published_at'),
    ),
    index_name: 'products',
    identifier: 'id',
);
```

`seal_schema_to_flow()` does the reverse conversion, from a SEAL schema back to a Flow schema.

### Type Mapping

| Flow Type                               | SEAL Field                         | Default Flags        |
|-----------------------------------------|------------------------------------|----------------------|
| `string`, `uuid`, `enum`, `xml`, `html` | `TextField`                        | searchable           |
| `integer`                               | `IntegerField`                     | filterable, sortable |
| `float`                                 | `FloatField`                       | filterable, sortable |
| `boolean`                               | `BooleanField`                     | filterable           |
| `datetime`, `date`                      | `DateTimeField`                    | filterable, sortable |
| `json`, `map`                           | `JsonObjectField`                  | -                    |
| `list<T>`                               | field of `T` with `multiple: true` | as `T`               |
| `structure`                             | `ObjectField` (recursive)          | -                    |

### Field Flags

You can override the default flags for each definition through its metadata:

| Metadata                     | Effect                                  |
|------------------------------|-----------------------------------------|
| `SealMetadata::identifier()` | marks the field as the index identifier |
| `SealMetadata::searchable()` | full-text searchable                    |
| `SealMetadata::filterable()` | usable in filters                       |
| `SealMetadata::sortable()`   | usable in sorting                       |
| `SealMetadata::distinct()`   | usable for distinct queries             |
| `SealMetadata::facet()`      | usable as a facet                       |

> Note: The index lifecycle (create and drop indexes) is a SEAL task. Use the engine directly:
> `$engine->createIndex('users')`, `$engine->dropIndex('users')`.

> Note: Some search backends index documents asynchronously. A document that the loader saved can be invisible to
> `countDocuments()` for a short time. Read the documentation of your backend. To wait for indexing tasks, SEAL has
> the `['return_slow_promise_result' => true]` option.
