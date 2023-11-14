# Upgrade Guide

This document provides guidelines for upgrading between versions of Flow PHP. 
Please follow the instructions for your specific version to ensure a smooth upgrade process.

---

## Upgrading from 0.4.x to 0.5.x

### 1) Entry factory moved from extractors to `FlowContext`

To improve code quality and reduce code coupling `EntryFactory` was removed from all constructors of extractors, in favor of passing it into `FlowContext` & re-using same entry factory in a whole pipeline.

### 2) Invalid schema has no fallback in `NativeEntryFactory`

Before, passing `Schema` into `NativeEntryFactory::create()` had fallback when the given entry was not found in a passed schema, now the schema has higher priority & fallback is no longer available, instead when the definition is missing in a passed schema, `InvalidArgumentException` will be thrown.

### 3) BufferLoader was removed

BufferLoader was removed in favor of `DataFrame::collect(int $batchSize = null)` method which now accepts additional
argument `$batchSize` that will keep collecting Rows from Extractor until the given batch size is reached.
Which does exactly the same thing as BufferLoader did, but in a more generic way.

### 4) Pipeline Closure 

Pipeline Closure was reduced to be only Loader Closure and it was moved to \Flow\ETL\Loader namespace. 
Additionally, \Closure::close method no longer requires Rows to be passed as an argument.

### 5) Parallelize 

DataFrame::parallelize() method is deprecated, and it will be removed, instead use DataFrame::batchSize(int $size) method. 

### 6) Rows in batch - Extractors

From now, file based Extractors will always throw one Row at time, in order to merge them into bigger groups
use `DataFrame::batchSize(int $size)` just after extractor method.

Before:
```php
<?php

(new Flow())
    ->read(CSV::from(__DIR__ . '/1_mln_rows.csv', rows_in_batch: 100))
    ->write(To::output())
    ->count();
```

After: 
```php
(new Flow())
    ->read(CSV::from(__DIR__ . '/1_mln_rows.csv',))
    ->batchSize(100)
    ->write(To::output())
    ->count();
```

Affected extractors:

- CSV
- Parquet
- JSON
- Text
- XML
- Avro
- DoctrineDBAL - rows_in_batch wasn't removed but now results are thrown row by row, instead of whole page. 
- GoogleSheet

### 7) GoogleSheetExtractor 

Argument `$rows_in_batch` was renamed to `$rows_per_page` which no longer determines the size of the batch, but the size of the page that will be fetched from Google API. 
Rows are yielded one by one. 

### 8) DataFrame::threadSafe() method was replaced by DataFrame::appendSafe()

`DataFrame::appendSafe()` is doing exactly the same thing as the old method, it's just more 
descriptive and self-explanatory. 
It's no longer mandatory to set this flat to true when using SaveMode::APPEND, it's now set automatically. 

### 9) Loaders - chunk size

Loaders are no longer accepting chunk_size parameter, from now in order to control 
the number of rows saved at once use `DataFrame::batchSize(int $size)` method.

### 10) Removed DSL functions: `datetime_string()`, `json_string()`

Those functions were removed in favor of accepting string values in related DSL functions:
- `datetime_string()` => `datetime()`,
- `json_string()` => `json()` & `json_object()`

### 11) Removed Asynchronous Processing

More details can be found in [this issue](https://github.com/flow-php/flow/issues/793). 

- Removed etl-adapter-amphp
- Removed etl-adapter-reactphp
- Removed LocalSocketPipeline
- Removed `DataFrame::pipeline()` 

---

## Upgrading from 0.3.x to 0.4.x

### 1) Transformers replaced with expressions

Transformers are a really powerful tool that was used in Flow since the beginning, but that tool was too powerful for the simple cases that were needed, and introduced additional complexity and maintenance issues when they were handwritten.

We reworked most of the internal transformers to new expressions and entry expressions (based on the built-in expressions), and we still internally use that powerful tool, but we don't expose it to end users, instead, we provide easy-to-use, covering all user needs expressions.

All available expressions can be found in [`ETL\Row\Reference\Expression` folder](src/core/etl/src/Flow/ETL/Row/Reference/Expression) or in [`ETL\DSL\functions` file](src/core/etl/src/Flow/ETL/DSL/functions.php), and entry expression are defined in [`EntryExpression` trait](src/core/etl/src/Flow/ETL/Row/Reference/EntryExpression.php).

To see what transformers are available see [`ETL\DSL\Transform` class](src/core/etl/src/Flow/ETL/DSL/Transform.php).

Before:
```php
<?php

use Flow\ETL\Extractor\MemoryExtractor;
use Flow\ETL\Flow;
use Flow\ETL\DSL\Transform;

(new Flow())
    ->read(new MemoryExtractor())
    ->rows(Transform::string_concat(['name', 'last name'], ' ', 'name'))
```

After:
```php
<?php

use function Flow\ETL\DSL\concat;
use function Flow\ETL\DSL\lit;
use Flow\ETL\Extractor\MemoryExtractor;
use Flow\ETL\Flow;

(new Flow())
    ->read(new MemoryExtractor())
    ->withEntry('name', concat(ref('name'), lit(' '), ref('last name')))
```

### 2) `ref` expression nullability 

`ref("entry_name")` is no longer returning null when the entry is not found. Instead, it throws an exception.
The same behavior can be achieved through using a newly introduced `optional` expression: 

Before:
```php
<?php

use function Flow\ETL\DSL\optional;
use function Flow\ETL\DSL\ref;

ref('non_existing_column')->cast('string'); 
```

After: 
```php
<?php

use function Flow\ETL\DSL\optional;
use function Flow\ETL\DSL\ref;

optional(ref('non_existing_column'))->cast('string');
// or  
optional(ref('non_existing_column')->cast('string'));
```

### 3) Extractors output

Affected extractors: 

* CSV
* JSON
* Avro
* DBAL
* GoogleSheet
* Parquet
* Text
* XML

Extractors are no longer returning data under an array entry called `row`, thanks to this unpacking row become redundant. 

Because of that all DSL functions are no longer expecting `$entry_row_name` parameter, if it was used anywhere,
please remove it. 

Before:
```php
<?php 

(new Flow())
    ->read(From::array([['id' => 1, 'array' => ['a' => 1, 'b' => 2, 'c' => 3]]]))
    ->withEntry('row', ref('row')->unpack())
    ->renameAll('row.', '')
    ->drop('row')
    ->withEntry('array', ref('array')->arrayMerge(lit(['d' => 4])))
    ->write(To::memory($memory = new ArrayMemory()))
    ->run();
```

After: 

```php
<?php

(new Flow())
    ->read(From::array([['id' => 1, 'array' => ['a' => 1, 'b' => 2, 'c' => 3]]]))
    ->withEntry('array', ref('array')->arrayMerge(lit(['d' => 4])))
    ->write(To::memory($memory = new ArrayMemory()))
    ->run();
```

### 4) ConfigBuilder::putInputIntoRows() output is now prefixed with _ (underscore)

In order to avoid collisions with datasets columns, additional columns created after using putInputIntoRows()
would now be prefixed with `_` (underscore) symbol. 

Before:
```php
<?php

$rows = (new Flow(Config::builder()->putInputIntoRows()))
            ->read(Json::from(__DIR__ . '/../Fixtures/timezones.json', 5))
            ->fetch();

foreach ($rows as $row) {
    $this->assertSame(
        [
            ...
            '_input_file_uri',
        ],
        \array_keys($row->toArray())
    );
}
```

After: 
```php
<?php

$rows = (new Flow(Config::builder()->putInputIntoRows()))
            ->read(Json::from(__DIR__ . '/../Fixtures/timezones.json', 5))
            ->fetch();

foreach ($rows as $row) {
    $this->assertSame(
        [
            ...
            '_input_file_uri',
        ],
        \array_keys($row->toArray())
    );
}
```
