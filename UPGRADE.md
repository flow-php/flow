# Upgrade Guide

This document provides guidelines for upgrading between versions of Flow PHP. 
Please follow the instructions for your specific version to ensure a smooth upgrade process.

---

## Upgrading from 0.4.x to 0.5.x

### 1) Entry factory moved from extractors to `FlowContext`

To improve code quality and reduce code coupling `EntryFactory` was removed from all constructors of extractors, in favor of passing it into `FlowContext` & re-using same entry factory in a whole pipeline.

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
