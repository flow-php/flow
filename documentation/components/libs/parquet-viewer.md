# Parquet Viewer

- [⬅️️ Back](../../introduction.md)
- [📚API Reference](/documentation/api/lib/parquet-viewer)
- [📁Files](/documentation/api/lib/parquet-viewer/indices/files.html)

## Installation

```
composer require flow-php/parquet-viewer:~--FLOW_PHP_VERSION--
```

Parquet Viewer is a simple CLI tool to inspect and view the content and metadata of parquet files. 

## Usage

```bash
./vendor/bin/parquet.php read:data /path/to/file.parquet
./vendor/bin/parquet.php read:metadata /path/to/file.parquet --columns --row-groups --column-chunks --statistics --page-headers
```