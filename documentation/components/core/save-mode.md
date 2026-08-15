# Save Mode

[DOC_LINK:/documentation/components/core/core]

[TOC]

Flow DataFrame provides four save modes that control how data is written when the destination file or path already exists:

- **ExceptionIfExists** (default): Throws an exception if the destination already exists
- **Append**: Appends data to existing files (may cause duplicates)
- **Overwrite**: Removes existing files and writes new data
- **Ignore**: Skips writing if destination already exists

## Changing Save Mode

Save mode is set using the same `DataFrame::mode()` method as ExecutionMode:

```php
use Flow\ETL\Filesystem\SaveMode;

(data_frame())
    ->read(from_array([
        ['id' => 1, 'name' => 'John'],
        ['id' => 2, 'name' => 'Jane'],
    ]))
    ->mode(SaveMode::Overwrite)
    ->write(to_csv(__DIR__ . '/output.csv'))
    ->run();
```

The save mode applies only to the DataFrame it is called on. DataFrames built from the same `Config` do not share it, so every DataFrame that needs a non-default mode must set its own:

```php
$config = config();

(data_frame($config))
    ->read(from_array([['id' => 1]]))
    ->mode(SaveMode::Overwrite)
    ->write(to_csv(__DIR__ . '/first.csv'))
    ->run();

(data_frame($config))
    ->read(from_array([['id' => 2]]))
    ->write(to_csv(__DIR__ . '/second.csv'))
    ->run();

// The second DataFrame runs with ExceptionIfExists, not Overwrite.
// Running it again throws:
// RuntimeException: Destination path "/path/to/second.csv" already exists
```

## Save Mode Behavior

### ExceptionIfExists (Default)

Fails immediately if the destination file already exists:

```php
(data_frame())
    ->read(from_array([['id' => 1]]))
    ->write(to_csv(__DIR__ . '/data.csv'))
    ->run();

// Running again throws:
// RuntimeException: Destination path "/path/to/data.csv" already exists
```

**Use when:** You want to ensure data is never accidentally overwritten.

### Append

Creates additional files in the same directory when destination already exists:

```php
(data_frame())
    ->read(from_array([['id' => 3]]))
    ->mode(SaveMode::Append)
    ->write(to_csv(__DIR__ . '/data.csv'))
    ->run();

// First run creates: data.csv
// Second run creates: data_<randomized_suffix>.csv (e.g., data_5f8a3b2c.csv)
// When reading data.csv, Flow reads all files matching the pattern: data*.csv
```

**How it works:**
- If destination file doesn't exist: writes normally
- If destination file exists: generates a new file with randomized name in the same directory
- Flow treats file paths as directories that can contain multiple files with the same extension

**File structure after multiple runs:**
```
output/
├── orders.csv              # First run
├── orders_5ea42a0310.csv   # Second run
├── orders_ceadbdb4d1.csv   # Third run
└── orders_2140bfc5fd.csv   # Fourth run
```

When you read from `orders.csv`, Flow automatically reads all `orders*.csv` files in the directory.

**Important:** Flow does not check for duplicates. If you run the same pipeline twice, data will be duplicated across multiple files.

**Use when:**
- Incrementally building datasets over multiple pipeline runs
- Writing to log directories
- Accumulating results where each run adds new files

### Overwrite

Removes all existing files at the destination and writes fresh data:

```php
(data_frame())
    ->read(from_array([['id' => 100]]))
    ->mode(SaveMode::Overwrite)
    ->write(to_csv(__DIR__ . '/data.csv'))
    ->run();

// File now contains only: id 100
```

**Implementation details:**
- Files are written to temporary files with `._flow_php_tmp.` prefix
- After writing completes, existing files are removed
- Temporary files are renamed to final names
- For partitioned writes, removes all files in partition directories

**Use when:**
- Regenerating reports or exports
- Running pipelines that should replace previous results
- Development and testing

### Ignore

Silently skips writing if the destination already exists:

```php
(data_frame())
    ->read(from_array([['id' => 999]]))
    ->mode(SaveMode::Ignore)
    ->write(to_csv(__DIR__ . '/data.csv'))
    ->run();

// If file exists: nothing happens, no error thrown
// If file doesn't exist: data is written normally
```

**Use when:**
- Idempotent pipelines where re-running should have no effect
- Avoiding duplicate work in batch processing
- Resume-like behavior for incremental processing

## Partitioned Writes

Save modes work with partitioned data:

```php
(data_frame())
    ->read(from_array([
        ['date' => '2024-01-01', 'value' => 100],
        ['date' => '2024-01-02', 'value' => 200],
    ]))
    ->mode(SaveMode::Overwrite)
    ->partitionBy('date')
    ->write(to_parquet(__DIR__ . '/data'))
    ->run();

// Structure:
// data/date=2024-01-01/file.parquet
// data/date=2024-01-02/file.parquet
```

When using `SaveMode::Overwrite` with partitions:
- All files within each affected partition directory are removed
- Only partitions being written to are affected
- Unrelated partitions remain untouched
