# Save Mode

[DOC_LINK:/documentation/components/core/core]

[TOC]

Flow provides four save modes that control how a file sink writes when the destination file or path already exists:

- **ExceptionIfExists** (default): Throws an exception if the destination already exists
- **Append**: Appends data to existing files (may cause duplicates)
- **Overwrite**: Removes existing files and writes new data
- **Ignore**: Skips writing if destination already exists

## Changing Save Mode

Save mode belongs to the sink, not to the DataFrame - set it with `FileLoader::saveMode()`:

```php
(data_frame())
    ->read(from_array([
        ['id' => 1, 'name' => 'John'],
        ['id' => 2, 'name' => 'Jane'],
    ]))
    ->write(to_csv(__DIR__ . '/output.csv')->saveMode(overwrite()))
    ->run();
```

Each sink carries its own mode, so two sinks in one DataFrame can differ and a mode never leaks from one
destination to another:

```php
(data_frame())
    ->read(from_array([['id' => 1]]))
    ->write(to_csv(__DIR__ . '/first.csv')->saveMode(overwrite()))
    ->write(to_csv(__DIR__ . '/second.csv'))
    ->run();

// The second sink runs with ExceptionIfExists, not Overwrite.
// Running it again throws:
// RuntimeException: Destination path "/path/to/second.csv" already exists
```

The registry a sink uses to track its open streams is scoped to one run, so a run that fails part-way does
not leave a stream registered for the next run to append into.

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
    ->write(to_csv(__DIR__ . '/data.csv')->saveMode(append()))
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
    ->write(to_csv(__DIR__ . '/data.csv')->saveMode(overwrite()))
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
    ->write(to_csv(__DIR__ . '/data.csv')->saveMode(ignore()))
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
    ->partitionBy('date')
    ->write(to_parquet(__DIR__ . '/data')->saveMode(overwrite()))
    ->run();

// Structure:
// data/date=2024-01-01/file.parquet
// data/date=2024-01-02/file.parquet
```

When using `SaveMode::Overwrite` with partitions:
- All files within each affected partition directory are removed
- Only partitions being written to are affected
- Unrelated partitions remain untouched
