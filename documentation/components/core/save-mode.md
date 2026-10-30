# Save Mode

[DOC_LINK:/documentation/components/core/core.md]

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
// Second run creates: data-<run>.csv (e.g., data-5f8a3b2c0d1e2f30.csv)
// Read them all with a glob: from_csv(__DIR__ . '/data*.csv')
```

**How it works:**
- If destination file doesn't exist: writes normally
- If destination file exists: writes `<name>-<run>.<ext>` beside it

**File structure after multiple runs:**
```
output/
├── orders.csv                        # First run
├── orders-5ea42a0310c3d4e5.csv       # Second run
├── orders-ceadbdb4d1a2b3c4.csv       # Third run
└── orders-2140bfc5fd6e7f80.csv       # Fourth run
```

Reading `orders.csv` reads that one file; read `orders*.csv` to get every run.

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
- Files are written under `.flow-tmp-<run>-<hash>/` inside the destination directory; readers skip it
- After writing completes, they are moved to their final names, then the files they replace are removed
- Only files Flow named are replaced: `<name>.<ext>`, `<name>-<run>.<ext>` and the `{part}` parts
- For partitioned writes, only the partitions that received rows are replaced
- A run without rows empties an unpartitioned destination

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

Ignore and ExceptionIfExists decide once for the whole destination: one existing file in any partition skips (or
refuses) the whole write.

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
    ->write(to_parquet(__DIR__ . '/data/file.parquet')
        ->partitionBy(partition_by(ref('date')))
        ->saveMode(overwrite()))
    ->run();

// Structure:
// data/date=2024-01-01/file.parquet
// data/date=2024-01-02/file.parquet
```

When using `SaveMode::Overwrite` with partitions:
- All files within each affected partition directory are removed
- Only partitions being written to are affected
- Unrelated partitions remain untouched

## Part destinations

A destination holding `{part}` is written as `part-00000`. The save mode is checked over every existing part of the
destination (`orders-*.parquet`), never over one file: `ExceptionIfExists` and `Ignore` see any part, `Overwrite`
removes every stale part before the new ones are renamed in, and `Append` names every part with the run id.
