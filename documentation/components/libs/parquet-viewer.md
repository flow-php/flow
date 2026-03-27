# Parquet Viewer

- [⬅️️ Back](/documentation/introduction.md)
- [📦Packagist](https://packagist.org/packages/flow-php/parquet-viewer)
- [➡️ Installation](/documentation/installation/packages/parquet-viewer.md)
- [🐙GitHub](https://github.com/flow-php/parquet-viewer)
- [📚API Reference](/documentation/api/lib/parquet-viewer)
- [📁Files](/documentation/api/lib/parquet-viewer/indices/files.html)

[TOC]

## Installation

For detailed installation instructions, see the [installation page](/documentation/installation/packages/parquet-viewer.md).

Parquet Viewer is a simple CLI tool to inspect and view the content and metadata of parquet files. 

## Usage

```bash
./vendor/bin/parquet.php read:data /path/to/file.parquet
./vendor/bin/parquet.php read:metadata /path/to/file.parquet --columns --row-groups --column-chunks --statistics --page-headers
```