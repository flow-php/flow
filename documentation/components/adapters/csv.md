---
package: flow-php/etl-adapter-csv
---

# CSV Adapter 

[PACKAGE_NAV]

[TOC]

Flow PHP's Adapter CSV is a proficient library crafted to enable seamless interaction with CSV data within your ETL (
Extract, Transform, Load) workflows. This adapter is indispensable for developers aiming to effortlessly extract from or
load data into CSV formats, ensuring a smooth and reliable data transformation journey. By employing the Adapter CSV
library, developers can access a robust set of features tailored for precise CSV data handling, making complex data
transformations both manageable and efficient. The Adapter CSV library encapsulates a broad range of functionalities,
providing a streamlined API for engaging with CSV data, which is vital in modern data processing and transformation
scenarios. This library embodies Flow PHP's dedication to offering versatile and effective data processing solutions,
making it a prime choice for developers dealing with CSV data in large-scale and data-intensive projects. With Flow
PHP's Adapter CSV, managing CSV data within your ETL workflows becomes a more simplified and efficient task, perfectly
aligning with the robust and adaptable framework of the Flow PHP ecosystem.

## Installation

For detailed installation instructions, see the [installation page](/documentation/installation/packages/etl-adapter-csv.md).

## Extractor

```php
<?php

$rows = data_frame()
    ->read(from_csv($path))
    ->fetch();
```

## Loader

```php 
<?php

data_frame()
    ->read(from_rows(
        rows(
            schema(int_schema('id'), str_schema('name')),
            row(['id' => 1, 'name' => 'Norbert']),
            row(['id' => 2, 'name' => 'Tomek']),
            row(['id' => 3, 'name' => 'Dawid']),
        )
    ))
    ->load(to_csv($path, true, true))
    ->run();
```
