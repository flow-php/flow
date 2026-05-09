---
package: flow-php/parquet-viewer
---

# Parquet Viewer

[PACKAGE_NAV]

[TOC]

## Installation

For detailed installation instructions, see the [installation page](/documentation/installation/packages/parquet-viewer.md).

Parquet Viewer is a simple CLI tool to inspect and view the content and metadata of parquet files. 

## Usage

```bash
./vendor/bin/parquet.php read:data /path/to/file.parquet
./vendor/bin/parquet.php read:metadata /path/to/file.parquet --columns --row-groups --column-chunks --statistics --page-headers
```