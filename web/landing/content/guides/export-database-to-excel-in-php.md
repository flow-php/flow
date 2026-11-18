---
seo_title: 'PhpSpreadsheet out of memory: export a database table to Excel in PHP'
seo_description: 'PhpSpreadsheet keeps the whole workbook in memory, so a big export ends with "Allowed memory size exhausted". Why it happens, and how to export a million rows to XLSX in 28Mb.'
group: Writing files
published: '2026-11-18'
updated: '2026-11-18'
---

# Export a database table to Excel in PHP without running out of memory

Let's say somebody from finance asks for all our orders as an Excel file.
There are a million of them in the database, and our PHP process runs with the default `memory_limit` of 128Mb.

> The scripts in this guide need a database and PhpSpreadsheet, so they don't run in the playground. Run them on your machine.

## The database

We need a table with orders, so let's create one. It's a SQLite file, so there is no server to set up.

```php ignore
<?php

declare(strict_types=1);

$rows = 1_000_000;
$database = new PDO('sqlite:' . __DIR__ . '/orders.sqlite');

$database->exec('DROP TABLE IF EXISTS orders');
$database->exec('CREATE TABLE orders (id INTEGER PRIMARY KEY, email TEXT, country TEXT, amount REAL, created_at TEXT)');

$insert = $database->prepare('INSERT INTO orders VALUES (?, ?, ?, ?, ?)');
$database->beginTransaction();

for ($id = 1; $id <= $rows; $id++) {
    $insert->execute([
        $id,
        "customer{$id}@example.com",
        ['PL', 'US', 'DE', 'FR', 'GB'][$id % 5],
        ($id % 1000) / 10,
        gmdate('Y-m-d H:i:s', 1_700_000_000 + $id * 60),
    ]);
}

$database->commit();

echo "orders.sqlite: {$rows} rows\n";
```

```text
orders.sqlite: 1000000 rows
```

## Export to Excel with PhpSpreadsheet

The first thing that comes to mind is PhpSpreadsheet.
We don't load the whole table. We fetch the orders one by one and add every order to the sheet.

```bash
composer require phpoffice/phpspreadsheet
```

```php ignore
<?php

declare(strict_types=1);

use PhpOffice\PhpSpreadsheet\Spreadsheet;
use PhpOffice\PhpSpreadsheet\Writer\Xlsx;

require __DIR__ . '/vendor/autoload.php';

$database = new PDO('sqlite:' . __DIR__ . '/orders.sqlite');
$orders = $database->query('SELECT id, email, country, amount, created_at FROM orders ORDER BY id');

$spreadsheet = new Spreadsheet();
$sheet = $spreadsheet->getActiveSheet();
$sheet->fromArray(['id', 'email', 'country', 'amount', 'created_at'], null, 'A1');
$line = 2;

while ($order = $orders->fetch(PDO::FETCH_NUM)) {
    $sheet->fromArray($order, null, 'A' . $line++);
}

(new Xlsx($spreadsheet))->save(__DIR__ . '/orders.xlsx');
```

With a hundred thousand orders it works, it just needs 367Mb to do it. With a million orders it ends like this:

```text
Fatal error: Allowed memory size of 134217728 bytes exhausted (tried to allocate 4096 bytes)
```

## Why PhpSpreadsheet ends with "Allowed memory size exhausted"

We fetch the orders one by one, so where does the memory go?

It goes to the output. PhpSpreadsheet keeps every cell of the workbook in memory until we call `save()`, so every order we add costs us more memory.
We could raise `memory_limit`, but for our million orders that means 3663Mb and more than a minute and a half of waiting.

## The same export with Flow

Flow needs PHP 8.3 or newer.

```bash
composer require flow-php/etl flow-php/etl-adapter-doctrine flow-php/etl-adapter-excel
```

```php ignore
<?php

declare(strict_types=1);

use function Flow\ETL\Adapter\Doctrine\{from_dbal_key_set_qb, pagination_key_asc, pagination_key_set};
use function Flow\ETL\Adapter\Excel\DSL\{from_excel, to_excel};
use function Flow\ETL\DSL\{data_frame, overwrite, to_output};
use Doctrine\DBAL\DriverManager;

require __DIR__ . '/vendor/autoload.php';

$database = DriverManager::getConnection(['driver' => 'pdo_sqlite', 'path' => __DIR__ . '/orders.sqlite']);

data_frame()
    ->read(from_dbal_key_set_qb(
        $database,
        $database->createQueryBuilder()->select('id', 'email', 'country', 'amount', 'created_at')->from('orders'),
        pagination_key_set(pagination_key_asc('id')),
    ))
    ->write(to_excel(__DIR__ . '/orders.xlsx')->saveMode(overwrite()))
    ->run();

data_frame()
    ->read(from_excel(__DIR__ . '/orders.xlsx'))
    ->select('id', 'email', 'amount')
    ->limit(3)
    ->write(to_output(truncate: false))
    ->run();
```

```text
+----+-----------------------+--------+
| id |                 email | amount |
+----+-----------------------+--------+
|  1 | customer1@example.com |    0.1 |
|  2 | customer2@example.com |    0.2 |
|  3 | customer3@example.com |    0.3 |
+----+-----------------------+--------+
3 rows
```

[`from_dbal_key_set_qb`](/documentation/dsl/doctrine/from-dbal-key-set-qb) reads the table page by page, and every page starts where the previous one ended.
[`to_excel`](/documentation/dsl/excel/to-excel) writes every order to the file right away, so nothing piles up in memory.
The second pipeline is there only to show us the first three rows of the file we just created.

`$database` is a Doctrine DBAL connection, so the same script exports a table from MySQL or PostgreSQL. Only the connection parameters change.

## How much memory does an Excel export take in PHP

PHP 8.3, `memory_limit=128M`, orders with five columns:

| Orders    | Code           | Peak memory               | Time |
|-----------|----------------|---------------------------|------|
| 100,000   | PhpSpreadsheet | 367Mb, without the limit  | 8.7s |
| 100,000   | Flow           | 28Mb                      | 3.8s |
| 1,000,000 | PhpSpreadsheet | fatal error, needs 3663Mb | 98s  |
| 1,000,000 | Flow           | 28Mb                      | 27s  |

Ten times more orders cost Flow nothing. They cost PhpSpreadsheet 3296Mb.

To check it on your machine, add this line at the end of any of the scripts above:

```php ignore
printf("peak memory: %.1f MB\n", memory_get_peak_usage(true) / 1024 / 1024);
```
