# Save Mode

[DOC_LINK:/documentation/components/core/core.md]

[TOC]

Flow has four save modes. They control how a file sink writes to a destination file or path that already exists:

- ExceptionIfExists (default): throws an exception for an existing destination
- Append: appends data to existing files (this can cause duplicates)
- Overwrite: removes existing files and writes new data
- Ignore: skips the write for an existing destination

## Changing Save Mode

Save mode belongs to the sink, not to the DataFrame. Set it with `FileLoader::saveMode()`:

```php
(data_frame())
    ->read(from_array([
        ['id' => 1, 'name' => 'John'],
        ['id' => 2, 'name' => 'Jane'],
    ]))
    ->write(to_csv(__DIR__ . '/output.csv')->saveMode(overwrite()))
    ->run();
```

Each sink carries its own mode. Two sinks in one DataFrame can therefore have different modes, and a mode never goes
from one destination to another:

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

A sink tracks its open streams in a registry that belongs to one run. A run that fails part of the way therefore
leaves no registered stream that the next run appends to.

## Save Mode Behavior

### ExceptionIfExists (Default)

Fails immediately for an existing destination file:

```php
(data_frame())
    ->read(from_array([['id' => 1]]))
    ->write(to_csv(__DIR__ . '/data.csv'))
    ->run();

// Running again throws:
// RuntimeException: Destination path "/path/to/data.csv" already exists
```

Use it to make sure that data is never overwritten by accident.

### Append

Creates more files in the same directory for an existing destination:

```php
(data_frame())
    ->read(from_array([['id' => 3]]))
    ->write(to_csv(__DIR__ . '/data.csv')->saveMode(append()))
    ->run();

// First run creates: data.csv
// Second run creates: data-<run>.csv (e.g., data-5f8a3b2c0d1e2f30.csv)
// Read them all with a glob: from_csv(__DIR__ . '/data*.csv')
```

How it works:
- If the destination file does not exist: writes normally
- If the destination file exists: writes `<name>-<run>.<ext>` next to it

File structure after multiple runs:
```
output/
├── orders.csv                        # First run
├── orders-5ea42a0310c3d4e5.csv       # Second run
├── orders-ceadbdb4d1a2b3c4.csv       # Third run
└── orders-2140bfc5fd6e7f80.csv       # Fourth run
```

A read of `orders.csv` reads that one file. To get all runs, read `orders*.csv`.

Important: Flow does not look for duplicates. If you run the same pipeline twice, the data is duplicated across multiple files.

Use it for:
- Datasets that grow over multiple pipeline runs
- Writes to log directories
- Results where each run adds new files

### Overwrite

Removes all existing files at the destination and writes new data:

```php
(data_frame())
    ->read(from_array([['id' => 100]]))
    ->write(to_csv(__DIR__ . '/data.csv')->saveMode(overwrite()))
    ->run();

// File now contains only: id 100
```

Implementation details:
- Files are written under `.flow-tmp-<run>-<hash>/` inside the destination directory. Readers skip it.
- After the write, they move to their final names, and then the files that they replace are removed.
- Only files Flow named are replaced: `<name>.<ext>`, `<name>-<run>.<ext>` and the `{part}` parts
- For partitioned writes, only the partitions that received rows are replaced
- A run without rows empties an unpartitioned destination

Use it for:
- Reports or exports that you generate again
- Pipelines that replace previous results
- Development and tests

### Ignore

Silently skips the write for an existing destination:

```php
(data_frame())
    ->read(from_array([['id' => 999]]))
    ->write(to_csv(__DIR__ . '/data.csv')->saveMode(ignore()))
    ->run();

// If file exists: nothing happens, no error thrown
// If file doesn't exist: data is written normally
```

Ignore and ExceptionIfExists decide once for the full destination. One existing file in any partition skips (or
refuses) the full write.

Use it for:
- Idempotent pipelines, where a second run has no effect
- No duplicate work in batch processing
- Resume-like behavior for incremental processing

## Partitioned Writes

Save modes work with partitioned data:

```php
(data_frame())
    ->read(from_array([
        ['date' => '2024-01-01', 'value' => 100],
        ['date' => '2024-01-02', 'value' => 200],
    ]))
    ->write(to_parquet(__DIR__ . '/data/file.parquet')
        ->partitionBy(partition_by(ref('date')))
        ->saveMode(overwrite()))
    ->run();

// Structure:
// data/date=2024-01-01/file.parquet
// data/date=2024-01-02/file.parquet
```

With `SaveMode::Overwrite` and partitions:
- All files within each affected partition directory are removed
- Only partitions being written to are affected
- Unrelated partitions remain untouched

## Part destinations

A destination with `{part}` is written as `part-00000`. The save mode applies to all existing parts of the
destination (`orders-*.parquet`), never to one file:

- `ExceptionIfExists` and `Ignore` see each part.
- `Overwrite` removes all stale parts before it renames the new ones.
- `Append` names each part with the run id.
