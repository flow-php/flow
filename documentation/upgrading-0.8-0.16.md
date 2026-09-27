# Upgrade Guide 0.8.x - 0.16.x

[TOC]

This document provides guidelines for upgrading between versions of Flow PHP. Please follow the instructions for your
specific version to ensure a smooth upgrade process.

Upgrading from older versions: [0.3.x to 0.8.x](/documentation/upgrading-0.3-0.8.md)

---

## Upgrading from 0.15.x to 0.16.x

### 1) Deprecated `Flow\ETL\DataFrame::renameAll*` methods

Methods:

- `Flow\ETL\DataFrame::renameAll()`,
- `Flow\ETL\DataFrame::renameAllLowerCase()`,
- `Flow\ETL\DataFrame::renameAllUpperCase()`,
- `Flow\ETL\DataFrame::renameAllUpperCaseFirst()`,
- `Flow\ETL\DataFrame::renameAllUpperCaseWord()`,

Were deprecated in favor of using new method: `DataFrame::renameEach()` with proper `RenameEntryStrategy` object.

### 2) Deprecated `RenameAllCaseTransformer` & `RenameStrReplaceAllEntriesTransformer`

Selected transformers were deprecated in favor of using `DataFrame::renameEach()` with related `RenameEntryStrategy`:

- `RenameAllCaseTransformer` -> `RenameCaseTransformer`,
- `RenameStrReplaceAllEntriesTransformer` -> `RenameReplaceStrategy`,

---

## Upgrading from 0.14.x to 0.15.x

### 1) Removed `Flow\ETL\Row\Schema\Matcher` and implementations

Schema Matcher was the initial attempt to implement a schema evolution next to schema validation that over time got
replaced with a different implementation of Schema Validator.

### 2) Renamed `Flow\ETL\Row\Schema` namespace into `Flow\ETL\Schema`.

This means all classes related to Schema now live under `Flow\ETL\Schema` namespace.

---

## Upgrading from 0.11.x to 0.14.x

### 1) Replaced `Flow\ETL\DataFrame::validate()` with `Flow\ETL\DataFrame::match()`

The old method is now deprecated and will be removed in the next release.

### 2) Replaced `Flow\ETL\Function\ScalarFunction\TypedScalarFunction` with

`Flow\ETL\Function\ScalarFunction\ScalarResult`.

The old interface was used to allow defining the return type of the ScalarFunctions. It was replaced with a ScalarResult
value object that is much more flexible than the interface, because it's allowing to return any type dynamically without
making the scalar function stateful.

---

## Upgrading from 0.10.x to 0.11.x

### 1) Removed StructureElement/struct_element/structure_element from StructureType Definition

Before:

```php
type_structure([
    struct_element('name', string()),
    struct_element('age', integer()),
]);
```

After:

```php
type_structure([
    'name' => string(),
    'age' => integer(),
]);
```

### 2) Doctrine DBAL Adapter

From now options for:

- `to_dbal_table_insert()`
- `to_db_table_update()`

are passed as objects (instance of UpdateOptions|InsertOptions interfaces) and they are platform specific, so please use
the proper class for the platform you are using.

- PostgreSQL
    - PostgreSQLInsertOptions
    - PostgreSQLUpdateOptions
- MySQL
    - MySQLInsertOptions
    - MySQLUpdateOptions
- Sqlite
    - SQLiteInsertOptions
    - SQLiteUpdateOptions

---

## Upgrading from 0.8.x to 0.10.x

### 1) Providing multiple paths to a single extractor

From now to read from multiple locations use `from_all(Extractor ...$extractors) : Exctractor` extractor.

Before:

```php
<?php

from_parquet([
    path(__DIR__ . '/data/1.parquet'),
    path(__DIR__ . '/data/2.parquet'),
]);
```

After:

```php
<?php

from_all(
    from_parquet(path(__DIR__ . '/data/1.parquet')),
    from_parquet(path(__DIR__ . '/data/2.parquet')),
);
```

### 2) Passing optional arguments to extractors/loaders

From now all extractors/loaders are accepting only mandatory arguments, all optional arguments should be passed through
`with*` methods and fluent interface.

Before:

```php
<?php

from_parquet(path(__DIR__ . '/data/1.parquet'), schema: $schema);
```

After:

```php
<?php

from_parquet(path(__DIR__ . '/data/1.parquet'))->withSchema($schema);
```
