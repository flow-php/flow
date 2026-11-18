---
seo_title: 'Import a CSV file into MySQL or PostgreSQL in PHP, with upsert'
seo_description: 'Import a CSV file into a MySQL or PostgreSQL table in PHP, a hundred thousand rows in under three seconds, and update the rows that are already there with an upsert.'
group: Importing data
published: '2026-11-18'
updated: '2026-11-18'
---

# Import a CSV file into MySQL or PostgreSQL in PHP

Let's say a CSV file with orders lands on our server every night, and we need to put it into the `orders` table.
Some of the orders are new, and some of them changed since yesterday.

> The scripts that talk to a database don't run in the playground. Run them on your machine.

## The file

First we need a file to import, so let's generate one.
Set `$rows` to `100_000` to get the version we measure below.

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

## The table

```sql tab="MySQL"
CREATE TABLE orders (
    id BIGINT PRIMARY KEY,
    email VARCHAR(255) NOT NULL,
    country VARCHAR(2) NOT NULL,
    amount DECIMAL(10, 2) NOT NULL,
    created_at DATETIME NOT NULL
);
```

```sql tab="PostgreSQL"
CREATE TABLE orders (
    id BIGINT PRIMARY KEY,
    email TEXT NOT NULL,
    country TEXT NOT NULL,
    amount NUMERIC(10, 2) NOT NULL,
    created_at TIMESTAMP NOT NULL
);
```

```php ignore tab="Flow PostgreSQL"
<?php

declare(strict_types=1);

use function Flow\PostgreSql\DSL\{
    column,
    column_type_bigint,
    column_type_numeric,
    column_type_text,
    column_type_timestamp,
    create,
    pgsql_client,
    pgsql_connection_dsn,
};

require __DIR__ . '/vendor/autoload.php';

$client = pgsql_client(pgsql_connection_dsn('postgres://shop:secret@127.0.0.1:5432/shop'));

$client->execute(
    create()->table('orders')
        ->ifNotExists()
        ->column(column('id', column_type_bigint())->primaryKey())
        ->column(column('email', column_type_text())->notNull())
        ->column(column('country', column_type_text())->notNull())
        ->column(column('amount', column_type_numeric(10, 2))->notNull())
        ->column(column('created_at', column_type_timestamp())->notNull())
);
```

The third tab creates the same PostgreSQL table with [flow-php/postgresql](/documentation/components/libs/postgresql), Flow's own PostgreSQL library. It has a query builder, so there is no SQL string in the script.

## Import a CSV file with one INSERT per row

The import everybody writes first: read a row, insert a row.

```php ignore tab="MySQL"
<?php

declare(strict_types=1);

$database = new PDO('mysql:host=127.0.0.1;dbname=shop', 'shop', 'secret', [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
$insert = $database->prepare('INSERT INTO orders (id, email, country, amount, created_at) VALUES (?, ?, ?, ?, ?)');

$file = fopen(__DIR__ . '/orders.csv', 'r');
fgetcsv($file, escape: '');

while (($row = fgetcsv($file, escape: '')) !== false) {
    $insert->execute($row);
}

fclose($file);
```

```php ignore tab="PostgreSQL"
<?php

declare(strict_types=1);

$database = new PDO('pgsql:host=127.0.0.1;dbname=shop', 'shop', 'secret', [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
$insert = $database->prepare('INSERT INTO orders (id, email, country, amount, created_at) VALUES (?, ?, ?, ?, ?)');

$file = fopen(__DIR__ . '/orders.csv', 'r');
fgetcsv($file, escape: '');

while (($row = fgetcsv($file, escape: '')) !== false) {
    $insert->execute($row);
}

fclose($file);
```

It works. For a hundred thousand orders it takes two minutes in PostgreSQL and more than four minutes in MySQL, because every order is a separate trip to the database.

## The second night it ends with a duplicate key error

The next night the file comes again, with the same orders and a few new ones:

```text tab="MySQL"
Fatal error: Uncaught PDOException: SQLSTATE[23000]: Integrity constraint violation: 1062 Duplicate entry '1' for key 'orders.PRIMARY'
```

```text tab="PostgreSQL"
Fatal error: Uncaught PDOException: SQLSTATE[23505]: Unique violation: 7 ERROR:  duplicate key value violates unique constraint "orders_pkey"
DETAIL:  Key (id)=(1) already exists.
```

The first order is already in the table, so the import stops at the first row.
What we need is an upsert: insert the order when it's new, update it when it's already there.
The SQL for that is different in every database. It's `ON DUPLICATE KEY UPDATE` in MySQL and `ON CONFLICT ... DO UPDATE` in PostgreSQL.

## Import and upsert a CSV file with Flow

Flow needs PHP 8.3 or newer.

```bash tab="Doctrine MySQL"
composer require flow-php/etl flow-php/etl-adapter-csv flow-php/etl-adapter-doctrine
```

```bash tab="Doctrine PostgreSQL"
composer require flow-php/etl flow-php/etl-adapter-csv flow-php/etl-adapter-doctrine
```

```bash tab="Flow PostgreSQL"
composer require flow-php/etl flow-php/etl-adapter-csv flow-php/etl-adapter-postgresql
```

And this is the whole import:

```php ignore tab="Doctrine MySQL"
<?php

declare(strict_types=1);

use function Flow\ETL\Adapter\CSV\from_csv;
use function Flow\ETL\Adapter\Doctrine\{mysql_insert_options, to_dbal_table_insert};
use function Flow\ETL\DSL\data_frame;
use Doctrine\DBAL\DriverManager;

require __DIR__ . '/vendor/autoload.php';

$database = DriverManager::getConnection([
    'driver' => 'pdo_mysql',
    'host' => '127.0.0.1',
    'dbname' => 'shop',
    'user' => 'shop',
    'password' => 'secret',
]);

data_frame()
    ->read(from_csv(__DIR__ . '/orders.csv'))
    ->write(to_dbal_table_insert($database, 'orders', mysql_insert_options(
        upsert: true,
        update_columns: ['email', 'country', 'amount', 'created_at'],
    )))
    ->run();
```

```php ignore tab="Doctrine PostgreSQL"
<?php

declare(strict_types=1);

use function Flow\ETL\Adapter\CSV\from_csv;
use function Flow\ETL\Adapter\Doctrine\{postgresql_insert_options, to_dbal_table_insert};
use function Flow\ETL\DSL\data_frame;
use Doctrine\DBAL\DriverManager;

require __DIR__ . '/vendor/autoload.php';

$database = DriverManager::getConnection([
    'driver' => 'pdo_pgsql',
    'host' => '127.0.0.1',
    'dbname' => 'shop',
    'user' => 'shop',
    'password' => 'secret',
]);

data_frame()
    ->read(from_csv(__DIR__ . '/orders.csv'))
    ->write(to_dbal_table_insert($database, 'orders', postgresql_insert_options(
        conflict_columns: ['id'],
        update_columns: ['email', 'country', 'amount', 'created_at'],
    )))
    ->run();
```

```php ignore tab="Flow PostgreSQL"
<?php

declare(strict_types=1);

use function Flow\ETL\Adapter\CSV\from_csv;
use function Flow\ETL\Adapter\PostgreSql\{pgsql_insert_options, to_pgsql_table};
use function Flow\ETL\DSL\data_frame;
use function Flow\PostgreSql\DSL\{pgsql_client, pgsql_connection_dsn};

require __DIR__ . '/vendor/autoload.php';

$client = pgsql_client(pgsql_connection_dsn('postgres://shop:secret@127.0.0.1:5432/shop'));

data_frame()
    ->read(from_csv(__DIR__ . '/orders.csv'))
    ->write(to_pgsql_table($client, 'orders')->withInsertOptions(pgsql_insert_options(
        conflictColumns: ['id'],
        updateColumns: ['email', 'country', 'amount', 'created_at'],
    )))
    ->run();
```

Flow doesn't send the orders one by one. It sends them in batches, many orders in one trip to the database.

The upsert is one option. We say which column tells us that the order is already there, and which columns to update, and Flow writes the right SQL for the database we use.
We can run the same script every night: new orders are inserted, and the ones that changed are updated.

The third tab doesn't use Doctrine. It writes through flow-php/postgresql, the same library that created the table above.

## How long does a CSV import take in PHP

PHP 8.3, a hundred thousand orders with five columns, the databases in Docker:

| Code                                 | MySQL | PostgreSQL |
|--------------------------------------|-------|------------|
| One `INSERT` per row                 | 264s  | 120s       |
| Flow with Doctrine                   | 2.4s  | 2.6s       |
| Flow with Doctrine, the second night | 1.9s  | 2.9s       |
| Flow PostgreSQL                      | -     | 2.3s       |
| Flow PostgreSQL, the second night    | -     | 2.5s       |
