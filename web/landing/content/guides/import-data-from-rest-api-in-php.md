---
seo_title: 'Consume a paginated REST API in PHP and keep the raw responses'
seo_description: 'Import data from a paginated REST API into a database in PHP without a pagination loop, and store every raw response, so we always know what the API sent.'
group: Importing data
problem: 'Nobody knows what the API sent'
published: '2026-11-18'
updated: '2026-11-18'
---

# Import data from a REST API in PHP and keep the raw responses

Let's say our orders live in somebody else's system, and every night we pull them from its REST API into the `orders` table.
The API gives a hundred orders per page, and it wants a token.

> These scripts need an API and a database, so they run only on your machine, not in the playground.

## The API

First we need an API to call, so let's create one. It's a single file for the web server that comes with PHP.

```php ignore
<?php

declare(strict_types=1);

header('Content-Type: application/json');

if (($_SERVER['HTTP_AUTHORIZATION'] ?? '') !== 'Bearer secret-token') {
    http_response_code(401);
    echo json_encode(['error' => 'invalid token']);

    return;
}

$total = 250;
$page = max(1, (int) ($_GET['page'] ?? 1));
$orders = [];

for ($id = ($page - 1) * 100 + 1; $id <= min($total, $page * 100); $id++) {
    $orders[] = [
        'id' => $id,
        'email' => "customer{$id}@example.com",
        'country' => ['PL', 'US', 'DE', 'FR', 'GB'][$id % 5],
        'amount' => ($id % 1000) / 10,
        'created_at' => gmdate('Y-m-d H:i:s', 1_700_000_000 + $id * 60),
    ];
}

echo json_encode(['data' => $orders, 'meta' => ['page' => $page, 'total' => $total]]);
```

```bash
php -S 127.0.0.1:8080 api.php
```

The `orders` table is the same as in [Import a CSV file into MySQL or PostgreSQL](/guides/import-csv-file-into-mysql-or-postgresql-in-php#the-table).

## Call a REST API in PHP with Guzzle

The first thing that comes to mind is a loop: ask for a page, decode it, insert the orders, ask for the next one.

```bash
composer require guzzlehttp/guzzle
```

```php ignore
<?php

declare(strict_types=1);

use GuzzleHttp\Client;

require __DIR__ . '/vendor/autoload.php';

$api = new Client([
    'base_uri' => 'http://127.0.0.1:8080',
    'headers' => ['Authorization' => 'Bearer secret-token'],
]);

$database = new PDO('mysql:host=127.0.0.1;dbname=shop', 'shop', 'secret');
$insert = $database->prepare(
    'INSERT INTO orders (id, email, country, amount, created_at) VALUES (?, ?, ?, ?, ?)
     ON DUPLICATE KEY UPDATE email = VALUES(email), country = VALUES(country), amount = VALUES(amount), created_at = VALUES(created_at)'
);

$page = 1;

do {
    $response = $api->get('/orders', ['query' => ['page' => $page]]);
    $orders = json_decode((string) $response->getBody(), true)['data'];

    foreach ($orders as $order) {
        $insert->execute([$order['id'], $order['email'], $order['country'], $order['amount'], $order['created_at']]);
    }

    $page++;
} while ($orders !== []);
```

It works, all 250 orders are in the table.

## Three weeks later an amount is wrong

Somebody from finance asks why order 187 has a different amount in our table than in the invoice.
Was it the API, or our code?

We can't tell. The response went through `json_decode` and five array keys into an `INSERT`, and after that it was gone.
The API shows today's version of the order, not the one it sent us three weeks ago.

With an SDK in place of Guzzle it's the same, only with objects between the response and the table.

So the import needs to keep what the API said. That means more code in the loop: a file to write to, the status code, the failed requests.
And the loop itself has to change for every API, because every API paginates in its own way.

## Consume the API with Flow

Flow needs PHP 8.3 or newer.

```bash
composer require flow-php/etl flow-php/etl-adapter-http flow-php/etl-adapter-json flow-php/etl-adapter-doctrine symfony/http-client nyholm/psr7
```

```php ignore
<?php

declare(strict_types=1);

use function Flow\ETL\Adapter\Doctrine\{mysql_insert_options, to_dbal_table_insert};
use function Flow\ETL\Adapter\Http\{from_http_paginated, http_pagination_page_number, http_request_option_query};
use function Flow\ETL\Adapter\JSON\to_json_lines;
use function Flow\ETL\DSL\{data_frame, datetime_schema, float_schema, int_schema, list_schema, lit, overwrite, ref, schema, select, str_schema, to_branch, to_transformation};
use function Flow\Types\DSL\{type_float, type_integer, type_list, type_string, type_structure};
use Doctrine\DBAL\DriverManager;
use Nyholm\Psr7\Factory\Psr17Factory;
use Symfony\Component\HttpClient\Psr18Client;

require __DIR__ . '/vendor/autoload.php';

$database = DriverManager::getConnection([
    'driver' => 'pdo_mysql',
    'host' => '127.0.0.1',
    'dbname' => 'shop',
    'user' => 'shop',
    'password' => 'secret',
]);

$order = type_structure([
    'id' => type_integer(),
    'email' => type_string(),
    'country' => type_string(),
    'amount' => type_float(),
    'created_at' => type_string(),
]);

data_frame()
    ->read(from_http_paginated(
        new Psr18Client(),
        (new Psr17Factory())
            ->createRequest('GET', 'http://127.0.0.1:8080/orders')
            ->withHeader('Authorization', 'Bearer secret-token'),
        http_pagination_page_number(http_request_option_query('page'), records_path: 'data'),
    ))
    ->write(to_transformation(
        select('request_uri', 'response_status_code', 'response_body'),
        to_json_lines(__DIR__ . '/responses-' . date('Y-m-d') . '.jsonl')->saveMode(overwrite()),
    ))
    ->write(to_branch(
        ref('response_status_code')->greaterThanEqual(lit(400)),
        to_transformation(
            select('request_uri', 'response_status_code', 'response_body'),
            to_json_lines(__DIR__ . '/failed-' . date('Y-m-d') . '.jsonl')->saveMode(overwrite()),
        ),
    ))
    ->filter(ref('response_status_code')->lessThan(lit(400)))
    ->withColumn('body', ref('response_body')->jsonDecode()->unpack(schema(list_schema('data', type_list($order)))))
    ->withColumn('order', ref('body.data')->expand())
    ->withColumn('order', ref('order')->unpack(schema(
        int_schema('id'),
        str_schema('email'),
        str_schema('country'),
        float_schema('amount'),
        datetime_schema('created_at'),
    )))
    ->select(
        ref('order.id')->as('id'),
        ref('order.email')->as('email'),
        ref('order.country')->as('country'),
        ref('order.amount')->as('amount'),
        ref('order.created_at')->as('created_at'),
    )
    ->write(to_dbal_table_insert($database, 'orders', mysql_insert_options(
        upsert: true,
        update_columns: ['email', 'country', 'amount', 'created_at'],
    )))
    ->run();
```

There is no loop. [`from_http_paginated`](/documentation/components/adapters/http) asks for `?page=1`, `?page=2` and so on, until the `data` of a response is empty.
Every response is one row, with the body exactly as the API sent it, the status code, the headers and the request that asked for it.

## Keep every raw response

The first `write` is the archive. It takes three columns of every response and puts them in a file with today's date, one response per line:

```text
{"request_uri":"http:\/\/127.0.0.1:8080\/orders?page=1","response_status_code":200,"response_body":"{\"data\":[{\"id\":1,\"email\":\"customer1@example.com\", ...
{"request_uri":"http:\/\/127.0.0.1:8080\/orders?page=2","response_status_code":200,"response_body":"{\"data\":[{\"id\":101,\"email\":\"customer101@example.com\", ...
{"request_uri":"http:\/\/127.0.0.1:8080\/orders?page=3","response_status_code":200,"response_body":"{\"data\":[{\"id\":201,\"email\":\"customer201@example.com\", ...
{"request_uri":"http:\/\/127.0.0.1:8080\/orders?page=4","response_status_code":200,"response_body":"{\"data\":[],\"meta\":{\"page\":4,\"total\":250}}"}
```

When finance asks about order 187, the answer is in the file from that night.

A `write` in the middle doesn't end anything. The same rows go on to the next step, so the archive and the import are one run and one pass over the API.

## Put the failed requests aside

The second `write` is a branch. [`to_branch`](/documentation/dsl/core/to-branch) takes a condition, and only the responses that match it go to its file.

With a wrong token the API answers 401. Guzzle throws an exception. Flow stops asking, and the response is in `failed-2026-11-18.jsonl`:

```text
{"request_uri":"http:\/\/127.0.0.1:8080\/orders?page=1","response_status_code":401,"response_body":"{\"error\":\"invalid token\"}"}
```

Nothing goes into the table, because the `filter` right after it lets only the good responses through.

## From responses to orders in the table

The rest turns pages into orders:

- `jsonDecode()->unpack(...)` decodes the body and says what is in it, a list of orders under `data`
- `expand()` makes one row per order out of one row per page
- the second `unpack` gives every order its columns and types, so `created_at` is a date and not a string
- `to_dbal_table_insert` with `upsert: true` inserts the new orders and updates the ones that changed, the same as in the [CSV import](/guides/import-csv-file-into-mysql-or-postgresql-in-php)

## A different API, a different pagination

Not every API counts pages. With the loop written by hand that means writing the loop again.
Here it's one argument:

```php ignore tab="Page number"
http_pagination_page_number(http_request_option_query('page'), records_path: 'data')
```

```php ignore tab="Offset"
http_pagination_offset(
    http_request_option_query('offset'),
    http_request_option_query('limit'),
    limit: 100,
    total_path: 'meta.total',
)
```

```php ignore tab="Cursor"
http_pagination_cursor('meta.next_cursor', http_request_option_query('cursor'))
```

```php ignore tab="Link header"
http_pagination_link_header('next')
```
