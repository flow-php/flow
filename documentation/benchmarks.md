# Benchmarks

- [⬅️️ Back](/documentation/introduction.md)

[TOC]

## Infrastructure

All benchmarks run on a dedicated self-hosted GitHub Actions runner hosted on [Digital Ocean](/sponsor):

| Specification   | Value                  |
|-----------------|------------------------|
| **Runner Name** | `flow-php-runner`      |
| **vCPUs**       | 2                      |
| **Memory**      | 2 GB                   |
| **OS**          | Ubuntu 24.04 (LTS) x64 |

Using a dedicated runner ensures benchmark results are consistent and not affected by varying loads on shared
GitHub-hosted runners.

## How Benchmarks Work

### Baseline Generation

When code is merged to the `1.x` branch, the baseline workflow automatically runs all benchmarks and stores the results
as artifacts. This baseline serves as the reference point for all future comparisons.

Baseline generation is triggered by:

- Push to `1.x` branch
- Daily schedule (3 AM UTC)
- Manual workflow dispatch

### Pull Request Comparison

When a pull request is opened, benchmarks run on the same dedicated runner and compare results against the stored `1.x`
baseline. This comparison is posted as a summary in the PR, allowing reviewers to identify any performance regressions
or improvements.

## Benchmark Categories

Benchmarks are organized into five categories:

| Category            | Description                                                                    |
|---------------------|--------------------------------------------------------------------------------|
| **Extractors**      | Performance of data extraction from various sources (CSV, JSON, Parquet, etc.) |
| **Transformers**    | Performance of data transformation operations                                  |
| **Loaders**         | Performance of data loading to various destinations                            |
| **Building Blocks** | Core framework operations (rows, entries, schema, etc.)                        |
| **Parquet Library** | Low-level Parquet file operations                                              |

## Running Benchmarks Locally

To run benchmarks locally:

```bash
# Run all benchmarks
composer test:benchmark

# Run specific category
composer test:benchmark:extractor
composer test:benchmark:transformer
composer test:benchmark:loader
composer test:benchmark:building_blocks
composer test:benchmark:parquet-library
```

## Interpreting Results

Benchmark results show timing comparisons between your changes and the baseline. Key metrics include:

- **Mean time** - Average execution time
- **Mode** - Most frequent execution time
- **Best/Worst** - Range of execution times
- **Memory** - Peak memory usage

A significant increase in execution time or memory usage may indicate a performance regression that should be
investigated before merging.
