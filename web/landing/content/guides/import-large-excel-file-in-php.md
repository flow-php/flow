---
seo_title: 'PhpSpreadsheet out of memory: import a large Excel file in PHP'
seo_description: 'PhpSpreadsheet builds an object for every cell, so a 3Mb Excel file runs out of memory. Why it happens, and how to read a million rows from XLSX in PHP with 26Mb.'
group: Importing data
problem: 'A 3Mb Excel file does not fit in 128Mb'
proof_label: 'Reading a 2.6Mb Excel file'
proof_before: '430Mb'
proof_after: '26Mb'
published: '2026-11-18'
updated: '2026-11-18'
---

# Import a large Excel file in PHP without running out of memory

Let's say a customer uploads their orders as an Excel file, and we need the revenue per country before we import them.
The file has a hundred thousand rows and it's small, 2.6Mb. Our PHP process runs with the default `memory_limit` of 128Mb.

## The file

First we need a file to read, so let's create one.
Set `$rows` to `100_000` to get the 2.6Mb version.

```php
<?php

declare(strict_types=1);

use function Flow\ETL\Adapter\Excel\DSL\to_excel;
use function Flow\ETL\DSL\{data_frame, from_array, overwrite};

require __DIR__ . '/vendor/autoload.php';

$rows = 1_000;
$orders = [];

for ($id = 1; $id <= $rows; $id++) {
    $orders[] = [
        'id' => $id,
        'email' => "customer{$id}@example.com",
        'country' => ['PL', 'US', 'DE', 'FR', 'GB'][$id % 5],
        'amount' => ($id % 1000) / 10,
        'created_at' => gmdate('Y-m-d H:i:s', 1_700_000_000 + $id * 60),
    ];
}

data_frame()
    ->read(from_array($orders))
    ->write(to_excel(__DIR__ . '/orders.xlsx')->saveMode(overwrite()))
    ->run();

echo "orders.xlsx: {$rows} rows\n";
```

```text output
orders.xlsx: 1000 rows
```

## Read an Excel file with PhpSpreadsheet

The first thing that comes to mind is PhpSpreadsheet: load the file and take the rows as an array.

> PhpSpreadsheet is not available in the playground, so this script runs only on your machine.

```bash
composer require phpoffice/phpspreadsheet
```

```php ignore
<?php

declare(strict_types=1);

use PhpOffice\PhpSpreadsheet\IOFactory;

require __DIR__ . '/vendor/autoload.php';

$sheet = IOFactory::load(__DIR__ . '/orders.xlsx')->getActiveSheet();
$revenue = [];

foreach ($sheet->toArray() as $number => $row) {
    if ($number === 0) {
        continue;
    }

    $revenue[$row[2]] = ($revenue[$row[2]] ?? 0.0) + (float) $row[3];
}

arsort($revenue);

foreach ($revenue as $country => $total) {
    printf("%s %.2f\n", $country, $total);
}
```

With a thousand rows it works. With a hundred thousand rows it ends like this:

```text
Fatal error: Allowed memory size of 134217728 bytes exhausted (tried to allocate 4096 bytes)
```

## Why a 3Mb Excel file ends with "Allowed memory size exhausted"

How can a file of 2.6Mb not fit in 128Mb?

An Excel file is a zip archive with XML inside, so it's much bigger than it looks.
And PhpSpreadsheet builds an object for every cell of the whole workbook before we get the first row. For our file that is 430Mb, more than 150 times the size of the file.

Laravel Excel (`maatwebsite/excel`) is built on PhpSpreadsheet, so the same reader is underneath.

## Read a large Excel file with Flow

Flow needs PHP 8.3 or newer.

```bash
composer require flow-php/etl flow-php/etl-adapter-excel
```

```php
<?php

declare(strict_types=1);

use function Flow\ETL\Adapter\Excel\DSL\from_excel;
use function Flow\ETL\DSL\{data_frame, ref, sum, to_output};

require __DIR__ . '/vendor/autoload.php';

data_frame()
    ->read(from_excel(__DIR__ . '/orders.xlsx'))
    ->groupBy([ref('country')])
    ->aggregate(sum(ref('amount')))
    ->sortBy(ref('amount_sum')->desc())
    ->write(to_output(truncate: false))
    ->run();
```

```text output
+---------+--------------+
| country |   amount_sum |
+---------+--------------+
|      GB | 10030.000000 |
|      FR | 10010.000000 |
|      DE |  9990.000000 |
|      US |  9970.000000 |
|      PL |  9950.000000 |
+---------+--------------+
5 rows
```

[`from_excel`](/documentation/dsl/excel/from-excel) reads the sheet row by row, so the workbook is never in memory as a whole.
`groupBy` with [`sum`](/documentation/dsl/core/sum) adds up the amounts per country, and `sortBy` puts the biggest one first.

From here, putting the orders into a database is the same as in [Import a CSV file into MySQL or PostgreSQL](/guides/import-csv-file-into-mysql-or-postgresql-in-php). Only the reader changes, `from_excel` in place of `from_csv`.

## How much memory does reading an Excel file take in PHP

PHP 8.3, `memory_limit=128M`, orders with five columns:

| Rows      | File size | Code           | Peak memory              | Time |
|-----------|-----------|----------------|--------------------------|------|
| 100,000   | 2.6Mb     | PhpSpreadsheet | fatal error, needs 430Mb | 8.0s |
| 100,000   | 2.6Mb     | Flow           | 26Mb                     | 3.7s |
| 1,000,000 | 26Mb      | Flow           | 26Mb                     | 30s  |

Ten times more rows cost Flow nothing.

To check it on your machine, add this line at the end of any of the scripts above:

```php ignore
printf("peak memory: %.1f MB\n", memory_get_peak_usage(true) / 1024 / 1024);
```
