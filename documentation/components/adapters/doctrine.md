---
package: flow-php/etl-adapter-doctrine
---

# ETL Adapter: Doctrine

[PACKAGE_NAV]

[TOC]

Flow PHP's Adapter Doctrine is an adept library designed to seamlessly integrate Doctrine ORM within your ETL (Extract,
Transform, Load) workflows. This adapter is crucial for developers seeking to effortlessly interact with databases using
Doctrine ORM, ensuring a streamlined and reliable data transformation process. By harnessing the Adapter Doctrine
library, developers can tap into a robust set of features engineered for precise database interaction through Doctrine
ORM, simplifying complex data transformations and enhancing data processing efficiency. The Adapter Doctrine library
encapsulates a rich set of functionalities, offering a streamlined API for managing database tasks, which is crucial in
contemporary data processing and transformation scenarios. This library epitomizes Flow PHP's commitment to delivering
versatile and efficient data processing solutions, making it an excellent choice for developers dealing with database
operations in large-scale and data-intensive environments. With Flow PHP's Adapter Doctrine, managing database
interactions within your ETL workflows becomes a more simplified and efficient endeavor, perfectly aligning with the
robust and adaptable nature of the Flow PHP ecosystem.

## Installation

For detailed installation instructions, see the [installation page](/documentation/installation/packages/etl-adapter-doctrine.md).

## Description

Adapter for [ETL](https://github.com/flow-php/etl) using bulk operations from [Doctrine Dbal Bulk](https://github.com/flow-php/doctrine-dbal-bulk).

## Loader - DbalLoader

```php
use Doctrine\DBAL\Tools\DsnParser;

use function Flow\ETL\Adapter\Doctrine\to_dbal_table_insert;
use function Flow\ETL\DSL\{data_frame, from_array};

$params = (new DsnParser(['postgresql' => 'pdo_pgsql']))->parse(\getenv('PGSQL_DATABASE_URL'));

data_frame()
    ->read(from_array([['id' => 1, 'name' => 'norbert']]))
    ->write(to_dbal_table_insert($params, 'users'))
    ->run();
```

All supported DbalLoader operations via DSL functions:

- `to_dbal_table_insert(array|Connection $connection, string $table, ?InsertOptions $options = null)` - Insert new rows
- `to_dbal_table_update(array|Connection $connection, string $table, ?UpdateOptions $options = null)` - Update existing rows
- `to_dbal_table_delete(array|Connection $connection, string $table)` - Delete rows

You can also configure bulk operations with platform-specific options:

```php
use function Flow\ETL\Adapter\Doctrine\{postgresql_insert_options, to_dbal_table_insert};
use function Flow\ETL\DSL\{data_frame, from_array};

data_frame()
    ->read(from_array([['id' => 1, 'name' => 'norbert']]))
    ->write(to_dbal_table_insert(
        $connection,
        'users',
        postgresql_insert_options(conflict_columns: ['id'])
    ))
    ->run();
```

### Types

By default, `DbalLoader` binds every column with the DBAL type mapped from its Flow type
(the default map is listed under [Schema Converter - Types Map](#schema-converter---types-map)):

```php
use function Flow\ETL\Adapter\Doctrine\to_dbal_table_insert;
use function Flow\ETL\DSL\{data_frame, from_array};

data_frame()
    ->read(from_array([['id' => 1, 'name' => 'norbert']]))
    ->write(to_dbal_table_insert($connection, 'users'))
    ->run();
```

#### Custom Types Map

A custom map replaces the default one, so spread `TypesMap::FLOW_TYPES` to keep the other types:

```php
use Doctrine\DBAL\Types\TextType;
use Flow\ETL\Adapter\Doctrine\TypesMap;
use Flow\Types\Type\Native\StringType;

use function Flow\ETL\Adapter\Doctrine\to_dbal_table_insert;
use function Flow\ETL\DSL\{data_frame, from_array};

$customTypesMap = new TypesMap([
    ...TypesMap::FLOW_TYPES,
    StringType::class => TextType::class,
]);

data_frame()
    ->read(from_array([['id' => 1, 'name' => 'norbert']]))
    ->write(to_dbal_table_insert($connection, 'users')
        ->withTypesMap($customTypesMap))
    ->run();
```

#### Bound values

A column is bound as its values and converted by its DBAL type: `list`, `map` and `structure` become JSON through
`JsonType`. `xml`, `xml_element`, `html` and `html_element` are bound as their markup text:

```php
use function Flow\ETL\Adapter\Doctrine\to_dbal_table_insert;
use function Flow\ETL\DSL\{data_frame, from_array, int_schema, schema, xml_schema};

data_frame()
    ->read(from_array(
        [['id' => 1, 'data' => '<root><item>1</item></root>']],
        schema(int_schema('id'), xml_schema('data')),
    ))
    ->write(to_dbal_table_insert($connection, 'xml_table'))
    ->run();
// data is bound as '<root><item>1</item></root>'
```

## Transactional Loading

`to_dbal_transaction()` writes one or more sinks inside transactions: each batch of rows is written in its own
transaction, and if any sink throws, the open transaction is rolled back:

```php
use function Flow\ETL\Adapter\Doctrine\{to_dbal_table_insert, to_dbal_transaction};
use function Flow\ETL\DSL\{data_frame, from_array};

data_frame()
    ->read(from_array([['id' => 1, 'name' => 'norbert']]))
    ->write(to_dbal_transaction(
        $connection,
        to_dbal_table_insert($connection, 'users'),
        to_dbal_table_insert($connection, 'users_audit'),
    ))
    ->run();
```

A plain loader child is a bare sink root; a `to_transformation(...)` child delivers inside the same transaction.
Every child's loader must use the same connection as the transaction - pass one live `Connection` to
`to_dbal_transaction()` and to every child. A loader built from array params (like
`to_dbal_table_insert($params, 'users')`) opens its own connection and escapes the transaction.

Sinks with blocking operations (`to_transformation()` / `to_branch(...)->withTransformation(...)` with `sortBy()`,
`aggregate()`, `groupBy()->aggregate()`, `pivot()`, window functions, `collect()`, `join()` - see
[transformations](../core/transformations.md)) buffer the stream and deliver it when the run ends;
`to_dbal_transaction()` opens one final transaction around that delivery - the whole drained stream commits
atomically, a failure during it rolls back and surfaces from `run()`.

Inside the transaction every sink runs throw-only: a failure rolls the batch back first, and only then the frame's
`onError()` handler decides whether the run continues.

To set the isolation level, build the transaction yourself; it applies to every transaction opened, including the
final one, and the previous level is restored when each one ends:

```php
use Doctrine\DBAL\TransactionIsolationLevel;
use Flow\ETL\Adapter\Doctrine\DbalTransaction;
use Flow\ETL\Sink\Transactional;

use function Flow\ETL\Adapter\Doctrine\to_dbal_table_insert;

new Transactional(
    DbalTransaction::fromConnection($connection)->withIsolationLevel(TransactionIsolationLevel::SERIALIZABLE),
    to_dbal_table_insert($connection, 'users'),
);
```

## Extractor - DbalQuery

`from_dbal_*()` extractors derive their schema from the query unless `->withSchema()` declares it - how that works
and fails: [Doctrine DBAL sources](../core/schema.md#doctrine-dbal-sources-describe-themselves-and-keep-withschema).

### Single Query

```php
use function Flow\ETL\Adapter\Doctrine\from_dbal_query;
use function Flow\ETL\DSL\{data_frame, to_output};

data_frame()
    ->read(from_dbal_query($connection, 'SELECT * FROM users ORDER BY id'))
    ->write(to_output())
    ->run();
```

### Single Parametrized Query

```php
use function Flow\ETL\Adapter\Doctrine\from_dbal_query;
use function Flow\ETL\DSL\{data_frame, to_output};

data_frame()
    ->read(from_dbal_query($connection, 'SELECT * FROM users WHERE id = :id', ['id' => 1]))
    ->write(to_output())
    ->run();
```

### Multiple Parametrized Queries

```php
use Flow\ETL\Adapter\Doctrine\ParametersSet;

use function Flow\ETL\Adapter\Doctrine\from_dbal_queries;
use function Flow\ETL\DSL\{data_frame, to_output};

data_frame()
    ->read(from_dbal_queries(
        $connection,
        'SELECT * FROM users ORDER BY id LIMIT :limit OFFSET :offset',
        new ParametersSet(
            ['limit' => 2, 'offset' => 0],
            ['limit' => 2, 'offset' => 2],
            ['limit' => 2, 'offset' => 4],
        ),
    ))
    ->write(to_output())
    ->run();
```

The query runs once per parameter set, three times here.

## Schema Converter

With `to_dbal_schema_table()` function we can convert any Flow Schema (which represents a dataset)
to Doctrine DBAL Schema Table.

By providing metadata defined in `\Flow\ETL\Adapter\Doctrine\DbalMetadata` we can also add additional information to the schema,
like length, primary key, index, precision, etc


```php
use Flow\ETL\Adapter\Doctrine\DbalMetadata;

use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\date_schema;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\json_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\map_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_string;

$flowSchema = schema(
    int_schema('int', nullable: false, metadata: DbalMetadata::primaryKey('pk_test')),
    str_schema('str', nullable: true, metadata: DbalMetadata::primaryKey('pk_test')),
    str_schema('str_with_length', true, DbalMetadata::length(255)),
    str_schema('str_unique', true, DbalMetadata::indexUnique('idx_str_unique')),
    date_schema('date', nullable: true, metadata: DbalMetadata::index('idx_date')),
    float_schema('float', nullable: true, metadata: DbalMetadata::precision(10)->merge(DbalMetadata::scale(2))),
    bool_schema('bool', nullable: true, metadata: DbalMetadata::default(true)),
    json_schema('json', nullable: true, metadata: DbalMetadata::platformOptions(['jsonb' => true])),
    list_schema('list', type_list(type_integer()), metadata: DbalMetadata::columnDefinition('integer[]')),
    map_schema('map', type_map(type_integer(), type_string()), metadata: DbalMetadata::comment('test comment!')),
);
```

Can be converted to Doctrine DBAL Schema Table like this:

```php
use function Flow\ETL\Adapter\Doctrine\to_dbal_schema_table;

to_dbal_schema_table($flowSchema, 'test');
```

Will generate:

```php

use Doctrine\DBAL\Schema\Column;
use Doctrine\DBAL\Schema\Index;
use Doctrine\DBAL\Schema\Table;
use Doctrine\DBAL\Types\Type;

new Table(
    'test',
    [
        new Column('int', Type::getType('integer'), ['notnull' => true]),
        new Column('str', Type::getType('string'), ['notnull' => true]),
        new Column('str_with_length', Type::getType('string'), ['notnull' => false, 'length' => 255]),
        new Column('str_unique', Type::getType('string'), ['notnull' => false]),
        new Column('date', Type::getType('date_immutable'), ['notnull' => false]),
        new Column('float', Type::getType('float'), ['notnull' => false, 'precision' => 10, 'scale' => 2]),
        new Column('bool', Type::getType('boolean'), ['notnull' => false, 'default' => true]),
        new Column('json', Type::getType('json'), ['notnull' => false, 'platformOptions' => ['jsonb' => true]]),
        new Column('list', Type::getType('json'), ['notnull' => true, 'columnDefinition' => 'integer[]']),
        new Column('map', Type::getType('json'), ['notnull' => true, 'comment' => 'test comment!']),
    ],
    [
        new Index('pk_test', ['int', 'str'], true, true),
        new Index('idx_date', ['date'], false, false),
        new Index('idx_str_unique', ['str_unique'], true, false),
    ]
);
```

### Schema Converter - Types Map

When types map is not provided, the default one will be used:

```php ignore
public const array FLOW_TYPES = [
    StringType::class => \Doctrine\DBAL\Types\StringType::class,
    IntegerType::class => \Doctrine\DBAL\Types\IntegerType::class,
    FloatType::class => \Doctrine\DBAL\Types\FloatType::class,
    BooleanType::class => \Doctrine\DBAL\Types\BooleanType::class,
    DateType::class => \Doctrine\DBAL\Types\DateImmutableType::class,
    TimeType::class => \Doctrine\DBAL\Types\TimeImmutableType::class,
    DateTimeType::class => \Doctrine\DBAL\Types\DateTimeImmutableType::class,
    UuidType::class => \Doctrine\DBAL\Types\GuidType::class,
    TimeZoneType::class => \Doctrine\DBAL\Types\StringType::class,
    JsonType::class => \Doctrine\DBAL\Types\JsonType::class,
    XMLType::class => \Doctrine\DBAL\Types\StringType::class,
    XMLElementType::class => \Doctrine\DBAL\Types\StringType::class,
    HTMLType::class => \Doctrine\DBAL\Types\StringType::class,
    HTMLElementType::class => \Doctrine\DBAL\Types\StringType::class,
    EnumType::class => \Doctrine\DBAL\Types\StringType::class,
    ListType::class => \Doctrine\DBAL\Types\JsonType::class,
    MapType::class => \Doctrine\DBAL\Types\JsonType::class,
    StructureType::class => \Doctrine\DBAL\Types\JsonType::class,
];
```

The `TypesMap` class provides bidirectional mapping between Flow types and Doctrine DBAL types, allowing for flexible type conversion in both directions:

```php
use Flow\ETL\Adapter\Doctrine\TypesMap;
use Flow\Types\Type\Native\StringType;
use Doctrine\DBAL\Types\TextType;

// Create custom type mapping
$customMap = new TypesMap([
    StringType::class => TextType::class,
]);

// Convert Flow type to DBAL type
$dbalType = $customMap->toDbalType(StringType::class);

// Convert DBAL type to Flow type instance
$flowType = $customMap->toFlowType(TextType::class);
```