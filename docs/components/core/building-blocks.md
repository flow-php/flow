# Building Blocks

- [⬅️️ Back](core.md)

Entries are the columns of the data frame, they are represented by the [Entry](../../../src/core/etl/src/Flow/ETL/Row/Entry.php) interface.
Group of Entries is called `Row`, it is represented by the [Row](../../../src/core/etl/src/Flow/ETL/Row.php) class.
Group of Rows is called `Rows`, it is represented by the [Rows](../../../src/core/etl/src/Flow/ETL/Rows.php) class.

Let's look at the following example: 

```php
<?php

declare(strict_types=1);

use function Flow\ETL\DSL\{bool_entry, int_entry, row, rows, str_entry};

$rows = rows(
    row(int_entry('id', 1), str_entry('name', 'user_01'), bool_entry('active', true)),
    row(int_entry('id', 2), str_entry('name', 'user_02'), bool_entry('active', false)),
    row(int_entry('id', 3), str_entry('name', 'user_03'), bool_entry('active', true)),
    row(int_entry('id', 3), str_entry('name', 'user_04'), bool_entry('active', false)),
);
```

Rows are the main data structure in Flow ETL, they’re used to represent data in the data frame.
Extractors are yielding Rows and Loaders are saving Rows. 

The same can be achieved using the following code:

```php
<?php

declare(strict_types=1);

use function Flow\ETL\DSL\array_to_rows;

$rows = array_to_rows([
    ['id' => 1, 'name' => 'user_01', 'active' => true],
    ['id' => 2, 'name' => 'user_02', 'active' => false],
    ['id' => 3, 'name' => 'user_03', 'active' => true],
    ['id' => 4, 'name' => 'user_04', 'active' => false],
]);
```

## Entry Types

- [Array](../../../src/core/etl/src/Flow/ETL/Row/Entry/ArrayEntry.php)
- [Boolean](../../../src/core/etl/src/Flow/ETL/Row/Entry/BooleanEntry.php)
- [DateTime](../../../src/core/etl/src/Flow/ETL/Row/Entry/DateTimeEntry.php)
- [Enum](../../../src/core/etl/src/Flow/ETL/Row/Entry/EnumEntry.php)
- [Float](../../../src/core/etl/src/Flow/ETL/Row/Entry/FloatEntry.php)
- [Integer](../../../src/core/etl/src/Flow/ETL/Row/Entry/IntegerEntry.php)
- [Json](../../../src/core/etl/src/Flow/ETL/Row/Entry/JsonEntry.php)
- [List](../../../src/core/etl/src/Flow/ETL/Row/Entry/ListEntry.php)
- [Map](../../../src/core/etl/src/Flow/ETL/Row/Entry/MapEntry.php)
- [Null](../../../src/core/etl/src/Flow/ETL/Row/Entry/NullEntry.php)
- [Object](../../../src/core/etl/src/Flow/ETL/Row/Entry/ObjectEntry.php)
- [String](../../../src/core/etl/src/Flow/ETL/Row/Entry/StringEntry.php)
- [Structure](../../../src/core/etl/src/Flow/ETL/Row/Entry/StructureEntry.php)
- [Uuid](../../../src/core/etl/src/Flow/ETL/Row/Entry/UuidEntry.php)
- [XML](../../../src/core/etl/src/Flow/ETL/Row/Entry/XMLEntry.php)
- [XMLNode](../../../src/core/etl/src/Flow/ETL/Row/Entry/XMLNodeEntry.php)

Internally flow is using [EntryFactory](../../../src/core/etl/src/Flow/ETL/Row/Factory/NativeEntryFactory.php) to create entries. 
It will try to detect and create the most appropriate entry type based on the value.

Flow Entries are based on [PHP Types](../../../src/core/etl/src/Flow/ETL/PHP/Type/Type.php), which are divided into two groups:

- Native
  - Array
  - Callable
  - Enum
  - Object
  - Resource
  - Scalar
- Logical
  - List
  - Map
  - Structure

