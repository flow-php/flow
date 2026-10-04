# Column Backend

[DOC_LINK:/documentation/components/core/core.md]

[TOC]

Every batch (`Rows`) stores one column per schema definition. The
[Backend](/src/core/etl/src/Flow/ETL/Column/Backend.php) decides where those columns live and builds them. A data
frame has exactly one backend, set on its config:

```php
<?php

declare(strict_types=1);

use Flow\ETL\Column\PhpBackend;

use function Flow\ETL\DSL\{config_builder, data_frame, from_array, lit, ref};

$rows = data_frame(config_builder()->backend(new PhpBackend()))
    ->read(from_array([['id' => 1], ['id' => 2]]))
    ->withEntry('double', ref('id')->multiply(lit(2)))
    ->fetch();

$rows->column('double')::class; // Flow\ETL\Column\Php\ScalarColumn
$rows->toArray();               // [['id' => 1, 'double' => 2], ['id' => 2, 'double' => 4]]
```

## Backends

| Backend                                                                  | Columns                       | Requires                                                                       |
|--------------------------------------------------------------------------|-------------------------------|--------------------------------------------------------------------------------|
| [AdaptiveBackend](/src/core/etl/src/Flow/ETL/Column/AdaptiveBackend.php) | `RustBackend` or `PhpBackend` | nothing (the default)                                                          |
| [PhpBackend](/src/core/etl/src/Flow/ETL/Column/PhpBackend.php)           | PHP arrays                    | nothing                                                                        |
| `RustBackend`                                                            | Apache Arrow arrays           | the [flow_php extension](/documentation/components/extensions/flow-php-ext.md) |

When `extension_loaded('flow_php')` is true, `AdaptiveBackend` picks `RustBackend`. Else it picks `PhpBackend`.
Both produce the same values: `PhpBackend` is the reference implementation, the extension is an accelerator. Set
`PhpBackend` explicitly to opt out of the extension while it is loaded.

## What the backend builds

The configured backend builds every column a pipeline produces:

- every batch an extractor yields: a batch built elsewhere is adopted (copied once) before the first step sees it,
- function results (`withEntry()`, `filter()`, `lit()`, ...),
- joins, sorts, group-by, window and repartition outputs, including their spilled reads,
- cache and Floe reads (the default `FloeSerializer` takes the same backend).

Code that builds batches outside a pipeline passes the backend itself - `$context->backend()` inside an extractor,
transformer or function, or any `Backend` to the DSL:

```php
<?php

declare(strict_types=1);

use Flow\ETL\Column\PhpBackend;

use function Flow\ETL\DSL\{array_to_rows, int_schema, schema};

$rows = array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id')), new PhpBackend());
```

## Memory

`RustBackend` keeps column buffers outside PHP's memory manager, where `memory_get_usage()` and `memory_limit` do not
see them. `Backend::allocatedBytes()` reports them, and Flow adds them to the process memory its spilling steps
(sort, join, group-by) compare with their memory limit.
