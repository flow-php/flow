---
seo_title: Read a large CSV file in PHP without running out of memory
seo_description: 'A CSV file bigger than the memory limit ends with "Allowed memory size exhausted". Why file() fails, how fgetcsv fixes it and what Flow adds, with code that runs.'
group: Reading files
published: '2026-11-18'
updated: '2026-11-18'
---

# Read a large CSV file in PHP without running out of memory

Let's say we got an export of orders: one CSV file, a million rows, 62Mb.
We need the revenue per country, and our PHP process runs with the default `memory_limit` of 128Mb.

Doesn't look like a complicated task, right?

## The file

First we need a file to work with, so let's generate one.
Set `$rows` to `1_000_000` to get the 62Mb version.

```php
<?php

declare(strict_types=1);

$rows = 1_000;
$file = fopen(__DIR__ . '/orders.csv', 'w');

fputcsv($file, ['id', 'email', 'country', 'amount', 'created_at'], escape: '');

for ($id = 1; $id <= $rows; $id++) {
    fputcsv($file, [
        $id,
        "customer{$id}@example.com",
        ['PL', 'US', 'DE', 'FR', 'GB'][$id % 5],
        number_format(($id % 1000) / 10, 2, '.', ''),
        gmdate('Y-m-d H:i:s', 1_700_000_000 + $id * 60),
    ], escape: '');
}

fclose($file);

echo "orders.csv: {$rows} rows\n";
```

```text output
orders.csv: 1000 rows
```

## Why file() ends with "Allowed memory size exhausted"

The first thing that comes to mind is `file()`. It reads the whole file into an array of lines, and then we loop over it.

```php
<?php

declare(strict_types=1);

$revenue = [];

foreach (file(__DIR__ . '/orders.csv') as $number => $line) {
    if ($number === 0) {
        continue;
    }

    $row = str_getcsv($line, escape: '');
    $revenue[$row[2]] = ($revenue[$row[2]] ?? 0.0) + (float) $row[3];
}

arsort($revenue);

foreach ($revenue as $country => $total) {
    printf("%s %.2f\n", $country, $total);
}
```

```text output
GB 10030.00
FR 10010.00
DE 9990.00
US 9970.00
PL 9950.00
```

With a thousand rows it works. With a million rows it ends like this:

```text
Fatal error: Allowed memory size of 134217728 bytes exhausted (tried to allocate 16777224 bytes)
```

`file()` keeps every line in memory at the same time. For our file that is 172Mb, and we have 128Mb.
We could raise `memory_limit`, but that only moves the problem to the next, bigger file.

## Read the CSV file line by line with fgetcsv

`fgetcsv` reads one row at a time, so it doesn't matter how big the file is.

```php
<?php

declare(strict_types=1);

$file = fopen(__DIR__ . '/orders.csv', 'r');
$header = fgetcsv($file, escape: '');
$revenue = [];

while (($row = fgetcsv($file, escape: '')) !== false) {
    $order = array_combine($header, $row);
    $revenue[$order['country']] = ($revenue[$order['country']] ?? 0.0) + (float) $order['amount'];
}

fclose($file);
arsort($revenue);

foreach ($revenue as $country => $total) {
    printf("%s %.2f\n", $country, $total);
}
```

```text output
GB 10030.00
FR 10010.00
DE 9990.00
US 9970.00
PL 9950.00
```

That's 2Mb of memory for a million rows. Reading a big file was never the hard part.

## Now sort it

OK, but the next request comes the same day: the same file, sorted by amount, the biggest orders first.

We can't sort a file one row at a time. To sort the rows we need all of them, so we are back to keeping the whole file in memory.

```php
<?php

declare(strict_types=1);

$file = fopen(__DIR__ . '/orders.csv', 'r');
$header = fgetcsv($file, escape: '');
$orders = [];

while (($row = fgetcsv($file, escape: '')) !== false) {
    $orders[] = array_combine($header, $row);
}

fclose($file);

usort($orders, static fn (array $left, array $right): int => (float) $right['amount'] <=> (float) $left['amount']);

$sorted = fopen(__DIR__ . '/orders-sorted.csv', 'w');
fputcsv($sorted, $header, escape: '');

foreach ($orders as $order) {
    fputcsv($sorted, $order, escape: '');
}

fclose($sorted);

foreach (array_slice($orders, 0, 3) as $order) {
    printf("%s %s %s\n", $order['id'], $order['email'], $order['amount']);
}
```

```text output
999 customer999@example.com 99.90
998 customer998@example.com 99.80
997 customer997@example.com 99.70
```

With a thousand rows it works. With a million rows we are back where we started:

```text
Fatal error: Allowed memory size of 134217728 bytes exhausted (tried to allocate 32768 bytes)
```

This time `fgetcsv` doesn't help. The script needs 624Mb for a 62Mb file, ten times its size, and it grows with every row.
To do it right we would have to cut the file into sorted chunks on disk and merge them back. That's a lot of code for "sort a file".

## The same thing with Flow

Flow needs PHP 8.3 or newer.

```bash
composer require flow-php/etl flow-php/etl-adapter-csv
```

```php
<?php

declare(strict_types=1);

use function Flow\ETL\Adapter\CSV\{from_csv, to_csv};
use function Flow\ETL\DSL\{data_frame, overwrite, ref, to_output};

require __DIR__ . '/vendor/autoload.php';

data_frame()
    ->read(from_csv(__DIR__ . '/orders.csv'))
    ->sortBy(ref('amount')->desc())
    ->write(to_csv(__DIR__ . '/orders-sorted.csv')->saveMode(overwrite()))
    ->run();

data_frame()
    ->read(from_csv(__DIR__ . '/orders-sorted.csv'))
    ->select('id', 'email', 'amount')
    ->limit(3)
    ->write(to_output(truncate: false))
    ->run();
```

```text output
+-----+-------------------------+-----------+
|  id |                   email |    amount |
+-----+-------------------------+-----------+
| 999 | customer999@example.com | 99.900000 |
| 998 | customer998@example.com | 99.800000 |
| 997 | customer997@example.com | 99.700000 |
+-----+-------------------------+-----------+
3 rows
```

[`from_csv`](/documentation/dsl/csv/from-csv) reads the file row by row, detects the separator and the type of every column, so `amount` is already a float.
[`sortBy`](/documentation/components/core/sort) keeps as many rows in memory as the limit allows and moves the rest to disk, so the script never needs more than it has.
The second pipeline is there only to show us the first three rows of the result.

The revenue report from the beginning is the same shape:

```php
<?php

declare(strict_types=1);

use function Flow\ETL\Adapter\CSV\from_csv;
use function Flow\ETL\DSL\{data_frame, ref, sum, to_output};

require __DIR__ . '/vendor/autoload.php';

data_frame()
    ->read(from_csv(__DIR__ . '/orders.csv'))
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

## How much memory does a large CSV file take in PHP

PHP 8.3, `memory_limit=128M`, a million rows, 62Mb:

| Task                | Code                                | Peak memory              | Time |
|---------------------|-------------------------------------|--------------------------|------|
| Revenue per country | `file()`                            | fatal error, needs 172Mb | -    |
| Revenue per country | `fgetcsv` loop                      | 2Mb                      | 1.6s |
| Revenue per country | Flow                                | 44Mb                     | 4.9s |
| Revenue per country | Flow with the `flow_php` extension  | 16Mb                     | 0.6s |
| Sort by amount      | `fgetcsv` and `usort`               | fatal error, needs 624Mb | -    |
| Sort by amount      | Flow                                | 98Mb                     | 7.4s |
| Sort by amount      | Flow with the `flow_php` extension  | 18Mb                     | 1.0s |

The numbers that matter are the ones that don't change. With three million rows and a 189Mb file the plain sort needs 1942Mb, and Flow still takes 97Mb, or 20Mb with the extension.

[flow_php](/documentation/components/extensions/flow-php-ext) is an optional PHP extension. The scripts stay the same, without a single change.

To check it on your machine, add this line at the end of any of the scripts above:

```php ignore
printf("peak memory: %.1f MB\n", memory_get_peak_usage(true) / 1024 / 1024);
```
