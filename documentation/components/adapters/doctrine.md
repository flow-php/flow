# ETL Adapter: Doctrine

- [⬅️️ Back](../../introduction.md)

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

```
composer require flow-php/etl-adapter-doctrine
```

## Description

Adapter for [ETL](https://github.com/flow-php/etl) using bulk operations from [Doctrine Dbal Bulk](https://github.com/flow-php/doctrine-dbal-bulk).

## Loader - DbalLoader

```php
data_frame()
    ->read(from_())
    ->write(new DbalLoader('your-table-name', $bulkSize = 100, ['url' => \getenv('PGSQL_DATABASE_URL')], ['skip_conflicts' => true]))
    ->run();
```

All supported types of `DbalBulkLoader` loading:

- `::insert(Connection $connection, string $table, QueryFactory $queryFactory = null) : self`
- `::insertOrSkipOnConflict(Connection $connection, string $table, QueryFactory $queryFactory = null) : self`
- `::insertOrUpdateOnConstraintConflict(Connection $connection, string $table, string $constraint, QueryFactory $queryFactory = null) : self`

The `bulkSize` means how many rows you want to push to a database in a single `INSERT` query. Each extracted rows set
is going to be split before inserting data into the database.

## Extractor - DbalQuery

This simple but powerful extractor let you extract data from a single or multiple parametrized queries.

### Single Query
```php 
data_frame()
    ->read(DbalQueryExtractor::singleQuery($connection, "SELECT * FROM {$table} ORDER BY id"))
    ->write(to_())
    ->run()
```

### Single Parametrized Query

```php 
data_frame()
    ->read(DbalQueryExtractor::singleQuery($connection, "SELECT * FROM {$table} WHERE id = :id", ['id' => 1]))
    ->write(to_())
    ->run()
```
### Multiple Parametrized Query

```php 
data_frame()
    ->read(
        new DbalQueryExtractor(
            $connection
            "SELECT * FROM {$table} ORDER BY id LIMIT :limit OFFSET :offset",
            new ParametersSet(
                ['limit' => 2, 'offset' => 0],
                ['limit' => 2, 'offset' => 2],
                ['limit' => 2, 'offset' => 4],
                ['limit' => 2, 'offset' => 6],
                ['limit' => 2, 'offset' => 8],
            )
        )
    )
    ->write(to_())
    ->run()
```

In this case, query will be executed exactly five times, taking every time next entry of parameters from ParametersSet.

## Schema Converter

With `to_dbal_schema_table()` function we can convert any Flow Schema (which represents a dataset)
to Doctrine DBAL Schema Table.

By providing metadata defined in `\Flow\ETL\Adapter\Doctrine\DbalMetadata` we can also add additional information to the schema,
like length, primary key, index, precision, etc


```php
use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\date_schema;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\json_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\map_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\type_integer;
use function Flow\ETL\DSL\type_list;
use function Flow\ETL\DSL\type_map;
use function Flow\ETL\DSL\type_string;

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

to_dbal_schema_table($flowSchema, 'test')
```

Will generate:

```php

use Doctrine\DBAL\Schema\Column;
use Doctrine\DBAL\Schema\Index;
use Doctrine\DBAL\Schema\Table;

new Table(
    'test',
    [
        new Column('int', Type::getType('integer'), ['notnull' => true]),
        new Column('str', Type::getType('string'), ['notnull' => false]),
        new Column('str_with_length', Type::getType('string'), ['notnull' => false, 'length' => 255]),
        new Column('str_unique', Type::getType('string'), ['notnull' => false]),
        new Column('float', Type::getType('float'), ['notnull' => false, 'precision' => 10, 'scale' => 2]),
        new Column('bool', Type::getType('boolean'), ['notnull' => false, 'default' => true]),
        new Column('json', Type::getType('json'), ['notnull' => false, 'platformOptions' => ['jsonb' => true]]),
        new Column('list', Type::getType('json'), ['notnull' => true, 'columnDefinition' => 'integer[]']),
        new Column('map', Type::getType('json'), ['notnull' => true, 'comment' => 'test comment!']),
        new Column('date', Type::getType('date_immutable'), ['notnull' => false]),
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

```php
public const DEFAULT_TYPES = [
    StringType::class => \Doctrine\DBAL\Types\StringType::class,
    IntegerType::class => \Doctrine\DBAL\Types\IntegerType::class,
    FloatType::class => \Doctrine\DBAL\Types\FloatType::class,
    BooleanType::class => \Doctrine\DBAL\Types\BooleanType::class,
    DateType::class => \Doctrine\DBAL\Types\DateImmutableType::class,
    TimeType::class => \Doctrine\DBAL\Types\TimeImmutableType::class,
    DateTimeType::class => \Doctrine\DBAL\Types\DateTimeImmutableType::class,
    UuidType::class => \Doctrine\DBAL\Types\GuidType::class,
    JsonType::class => \Doctrine\DBAL\Types\JsonType::class,
    XMLType::class => \Doctrine\DBAL\Types\StringType::class,
    XMLElementType::class => \Doctrine\DBAL\Types\StringType::class,
    ListType::class => \Doctrine\DBAL\Types\JsonType::class,
    MapType::class => \Doctrine\DBAL\Types\JsonType::class,
    StructureType::class => \Doctrine\DBAL\Types\JsonType::class,
];
```