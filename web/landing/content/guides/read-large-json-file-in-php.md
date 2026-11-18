---
seo_title: Read a large JSON file in PHP without running out of memory
seo_description: 'json_decode needs the whole file in memory, so a big JSON file ends with "Allowed memory size exhausted". Why it happens, and how to read a 110Mb JSON file in PHP with 22Mb.'
group: Reading files
published: '2026-11-18'
updated: '2026-11-18'
---

# Read a large JSON file in PHP without running out of memory

Let's say a partner sends us their orders as one JSON file: a million orders, 110Mb.
We need the revenue per country, and our PHP process runs with the default `memory_limit` of 128Mb.

## The file

First we need a file to work with, so let's generate one.
Set `$rows` to `1_000_000` to get the 110Mb version.

```php
<?php

declare(strict_types=1);

$rows = 1_000;
$file = fopen(__DIR__ . '/orders.json', 'w');

fwrite($file, "[\n");

for ($id = 1; $id <= $rows; $id++) {
    fwrite($file, json_encode([
        'id' => $id,
        'email' => "customer{$id}@example.com",
        'country' => ['PL', 'US', 'DE', 'FR', 'GB'][$id % 5],
        'amount' => ($id % 1000) / 10,
        'created_at' => gmdate('Y-m-d H:i:s', 1_700_000_000 + $id * 60),
    ]) . ($id < $rows ? ",\n" : "\n"));
}

fwrite($file, "]\n");
fclose($file);

echo "orders.json: {$rows} orders\n";
```

```text output
orders.json: 1000 orders
```

## Read a JSON file with json_decode

There is one way to read JSON in PHP that everybody knows: `file_get_contents` and `json_decode`.

```php
<?php

declare(strict_types=1);

$orders = json_decode(file_get_contents(__DIR__ . '/orders.json'), true, flags: JSON_THROW_ON_ERROR);
$revenue = [];

foreach ($orders as $order) {
    $revenue[$order['country']] = ($revenue[$order['country']] ?? 0.0) + $order['amount'];
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

With a thousand orders it works. With a million orders it ends like this:

```text
Fatal error: Allowed memory size of 134217728 bytes exhausted (tried to allocate 20480 bytes)
```

## Why json_decode ends with "Allowed memory size exhausted"

`json_decode` can't read a part of a file. It needs the whole document as one string, and then it builds a PHP array for every order in it.
For our file that is 778Mb, seven times the size of the file.

With a CSV file we could switch to `fgetcsv` and read it row by row. There is nothing like that for JSON.
PHP has no function that reads a JSON file one element at a time, so we would have to write our own parser.

## Read a large JSON file with Flow

Flow needs PHP 8.3 or newer.

```bash
composer require flow-php/etl flow-php/etl-adapter-json
```

```php
<?php

declare(strict_types=1);

use function Flow\ETL\Adapter\JSON\from_json;
use function Flow\ETL\DSL\{data_frame, ref, sum, to_output};

require __DIR__ . '/vendor/autoload.php';

data_frame()
    ->read(from_json(__DIR__ . '/orders.json'))
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

[`from_json`](/documentation/dsl/json/from-json) reads the file one order at a time, so it doesn't matter how big the file is.
`groupBy` with [`sum`](/documentation/dsl/core/sum) adds up the amounts per country, and `sortBy` puts the biggest one first.

## How much memory does a large JSON file take in PHP

PHP 8.3, `memory_limit=128M`:

| Orders    | File size | Code                               | Peak memory               | Time |
|-----------|-----------|------------------------------------|---------------------------|------|
| 1,000,000 | 110Mb     | `json_decode`                      | fatal error, needs 778Mb  | -    |
| 1,000,000 | 110Mb     | Flow                               | 22Mb                      | 2.5s |
| 1,000,000 | 110Mb     | Flow with the `flow_php` extension | 20Mb                      | 1.0s |
| 3,000,000 | 335Mb     | `json_decode`                      | fatal error, needs 2353Mb | -    |
| 3,000,000 | 335Mb     | Flow                               | 22Mb                      | 6.8s |
| 3,000,000 | 335Mb     | Flow with the `flow_php` extension | 20Mb                      | 2.2s |

Three times more orders cost Flow nothing. They cost `json_decode` 1575Mb.

[flow_php](/documentation/components/extensions/flow-php-ext) is an optional PHP extension. The script stays the same, without a single change.

To check it on your machine, add this line at the end of any of the scripts above:

```php ignore
printf("peak memory: %.1f MB\n", memory_get_peak_usage(true) / 1024 / 1024);
```
