---
package: flow-php/etl-adapter-seal
---

# ETL Adapter: Seal

[PACKAGE_NAV]

[TOC]

Flow PHP Adapter for [SEAL](https://php-cmsig.github.io/search/) - a PHP Search Engine Abstraction Layer that provides a
unified API for working with multiple search engines (Algolia, Elasticsearch, Meilisearch, OpenSearch, Solr, Typesense,
RediSearch, Loupe). This adapter brings SEAL's engine-agnostic search capabilities into your Flow PHP ETL workflows,
allowing you to extract from or load data into any SEAL-supported search backend without coupling your pipelines to a
specific search engine implementation.

## Installation

For detailed installation instructions, see the [installation page](/documentation/installation/packages/etl-adapter-seal.md).

In addition to this adapter, install the SEAL engine adapter for your search backend, for example:

```
composer require flow-php/etl-adapter-seal cmsig/seal-elasticsearch-adapter
```

| Backend       | Package                          |
|---------------|----------------------------------|
| Algolia       | `cmsig/seal-algolia-adapter`     |
| Elasticsearch | `cmsig/seal-elasticsearch-adapter` |
| Loupe         | `cmsig/seal-loupe-adapter`       |
| Meilisearch   | `cmsig/seal-meilisearch-adapter` |
| Memory        | `cmsig/seal-memory-adapter`      |
| OpenSearch    | `cmsig/seal-opensearch-adapter`  |
| RediSearch    | `cmsig/seal-redisearch-adapter`  |
| Solr          | `cmsig/seal-solr-adapter`        |
| Typesense     | `cmsig/seal-typesense-adapter`   |

## Description

The adapter operates on a `CmsIg\Seal\EngineInterface` that you build yourself from the SEAL adapter of your choice and
a SEAL schema. This works the same way as the Doctrine and PostgreSQL adapters, which accept a pre-configured
connection/client - and it plays well with the [SEAL Symfony bundle](https://php-cmsig.github.io/search/integrations/symfony.html),
where a fully configured engine can simply be injected.

- **Loader** - `to_seal_upsert()` / `to_seal_delete()` write or remove documents in a search index in bulks.
- **Schema Conversion** - `to_seal_schema()` / `seal_schema_to_flow()` convert between Flow and SEAL schemas.

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

`to_seal_upsert()` saves documents into the index. Search engines upsert by document identifier - loading a document
with an existing identifier replaces it.

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

Documents are written in bulks of 100 by default, adjustable with `withBulkSize()`:

```php
to_seal_upsert($engine, 'users')->withBulkSize(500);
```

### Deleting Documents

`to_seal_delete()` removes documents by their identifiers. By default, the identifier is taken from the `id` entry of
each row - use `withIdentifierEntry()` when your identifier entry has a different name.

```php
use function Flow\ETL\Adapter\Seal\to_seal_delete;
use function Flow\ETL\DSL\{data_frame, from_array};

data_frame()
    ->read(from_array([['id' => '1'], ['id' => '2']]))
    ->write(to_seal_delete($engine, 'users'))
    ->run();
```

```php
to_seal_delete($engine, 'products')->withIdentifierEntry('sku');
```

### Data Normalization

Before saving, rows are normalized into search documents:

| Flow Type                 | Document Value                                  |
|---------------------------|-------------------------------------------------|
| `string`, `int`, `float`, `bool` | unchanged                                 |
| `uuid`                    | string representation                           |
| `datetime`                | string, `DateTimeInterface::ATOM` by default     |
| `date`                    | string, `Y-m-d` by default                       |
| `enum`                    | case name                                       |
| `xml`, `xml_element`      | serialized XML string                           |
| `json`                    | decoded array                                   |
| `list`, `map`, `structure` | array (nested values normalized recursively)   |

## Schema Conversion

`to_seal_schema()` converts a Flow schema into a SEAL schema for a single index. SEAL requires exactly one identifier
field per index - name it with the `identifier` argument or mark a definition with `SealMetadata::identifier()`.

```php
use Flow\ETL\Adapter\Seal\SealMetadata;

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

`seal_schema_to_flow()` performs the reverse conversion, from a SEAL schema back to a Flow schema.

### Type Mapping

| Flow Type                          | SEAL Field        | Default Flags          |
|------------------------------------|-------------------|------------------------|
| `string`, `uuid`, `enum`, `xml`, `html` | `TextField`  | searchable             |
| `integer`                          | `IntegerField`    | filterable, sortable   |
| `float`                            | `FloatField`      | filterable, sortable   |
| `boolean`                          | `BooleanField`    | filterable             |
| `datetime`, `date`                 | `DateTimeField`   | filterable, sortable   |
| `json`, `map`                      | `JsonObjectField` | -                      |
| `list<T>`                          | field of `T` with `multiple: true` | as `T` |
| `structure`                        | `ObjectField` (recursive) | -              |

### Field Flags

Default flags can be overridden per definition through entry metadata:

| Metadata                     | Effect                                  |
|------------------------------|-----------------------------------------|
| `SealMetadata::identifier()` | marks the field as the index identifier |
| `SealMetadata::searchable()` | full-text searchable                    |
| `SealMetadata::filterable()` | usable in filters                       |
| `SealMetadata::sortable()`   | usable in sorting                       |
| `SealMetadata::distinct()`   | usable for distinct queries             |
| `SealMetadata::facet()`      | usable as a facet                       |

> **Note:** Index lifecycle (creating/dropping indexes) is a SEAL concern - use the engine directly:
> `$engine->createIndex('users')`, `$engine->dropIndex('users')`.

> **Note:** Some search backends index documents asynchronously - a document saved by the loader may not be
> immediately visible to `countDocuments()`. Consult your backend's documentation; SEAL exposes the
> `['return_slow_promise_result' => true]` option to wait for indexing tasks.
