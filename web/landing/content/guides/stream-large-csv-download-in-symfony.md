---
seo_title: 'Export a large CSV file in PHP: stream the download in Symfony without running out of memory'
seo_description: 'A Symfony StreamedResponse still loads the whole query result, so a big export ends with "Allowed memory size exhausted". How to stream a million rows as CSV, JSON, XML or Parquet in 20Mb.'
group: Writing files
published: '2026-11-18'
updated: '2026-11-18'
---

# Stream a large CSV download in Symfony without running out of memory

Let's say our application has a "Download orders" button.
There are a million orders in MySQL, and our PHP process runs with the default `memory_limit` of 128Mb.

> The controllers in this guide need a Symfony application and a database, so they don't run in the playground. Run them in your project.

## Export a CSV file with a Symfony StreamedResponse

Symfony has a response made for this. We don't build the file first. We send every order to the browser as soon as we read it.

```php ignore
<?php

declare(strict_types=1);

namespace App\Controller;

use Doctrine\DBAL\Connection;
use Symfony\Component\HttpFoundation\StreamedResponse;
use Symfony\Component\Routing\Attribute\Route;

final class OrdersController
{
    public function __construct(
        private readonly Connection $database,
    ) {}

    #[Route('/orders/download', name: 'orders_download')]
    public function download(): StreamedResponse
    {
        return new StreamedResponse(function (): void {
            $output = fopen('php://output', 'w');
            fputcsv($output, ['id', 'email', 'country', 'amount', 'created_at'], escape: '');

            $orders = $this->database->executeQuery('SELECT id, email, country, amount, created_at FROM orders ORDER BY id');

            while ($order = $orders->fetchAssociative()) {
                fputcsv($output, $order, escape: '');
            }
        }, 200, [
            'Content-Type' => 'text/csv',
            'Content-Disposition' => 'attachment; filename="orders.csv"',
        ]);
    }
}
```

It looks right. Nothing is collected in an array, and every order goes straight to the output.

## Why StreamedResponse still ends with "Allowed memory size exhausted"

Let's look at how much memory that controller takes:

| Orders    | Peak memory |
|-----------|-------------|
| 500,000   | 52Mb        |
| 1,000,000 | 91Mb        |
| 2,000,000 | fatal error |

The more orders we have, the more memory it takes, and at two million it ends with:

```text
Fatal error: Allowed memory size of 134217728 bytes exhausted
```

The response is streamed, but the query is not. The MySQL driver reads the whole result into memory before we get the first order from it.
So the download works on our machine, works for the first year in production, and then one day it stops.

## Then somebody asks for JSON or XML

The next request is always the same: "can we get it as JSON?".
There is nothing to reuse from the CSV controller. Every format is its own code:

```php ignore tab="JSON"
return new StreamedResponse(function (): void {
    $orders = $this->database->executeQuery('SELECT id, email, country, amount, created_at FROM orders ORDER BY id');
    $first = true;

    echo '[';

    while ($order = $orders->fetchAssociative()) {
        $order['amount'] = (float) $order['amount'];

        echo $first ? '' : ',', json_encode($order, JSON_THROW_ON_ERROR);
        $first = false;
    }

    echo ']';
}, 200, [
    'Content-Type' => 'application/json',
    'Content-Disposition' => 'attachment; filename="orders.json"',
]);
```

```php ignore tab="XML"
return new StreamedResponse(function (): void {
    $orders = $this->database->executeQuery('SELECT id, email, country, amount, created_at FROM orders ORDER BY id');

    $xml = new XMLWriter();
    $xml->openUri('php://output');
    $xml->startDocument('1.0', 'UTF-8');
    $xml->startElement('rows');

    while ($order = $orders->fetchAssociative()) {
        $xml->startElement('row');

        foreach ($order as $column => $value) {
            $xml->writeElement($column, (string) $value);
        }

        $xml->endElement();
        $xml->flush();
    }

    $xml->endElement();
    $xml->endDocument();
    $xml->flush();
}, 200, [
    'Content-Type' => 'application/xml',
    'Content-Disposition' => 'attachment; filename="orders.xml"',
]);
```

For JSON we write the brackets and the commas ourselves, and we cast the amount, because the database gives it to us as a string.
For XML we need `XMLWriter`, and we have to flush it after every order, or the whole document waits in memory.
And both of them still read the whole query result first, so they take the same 91Mb.

## Export and stream the download with Flow

Flow needs PHP 8.3 or newer.

```bash
composer require flow-php/etl flow-php/etl-adapter-doctrine flow-php/symfony-http-foundation-bridge
```

```php ignore
<?php

declare(strict_types=1);

namespace App\Controller;

use function Flow\Bridge\Symfony\HttpFoundation\{http_csv_output, http_stream_open};
use function Flow\ETL\Adapter\Doctrine\{from_dbal_key_set_qb, pagination_key_asc, pagination_key_set};
use function Flow\ETL\DSL\{
    datetime_schema,
    float_schema,
    int_schema,
    mask_columns,
    ref,
    schema,
    str_schema,
    with_column,
};
use Doctrine\DBAL\Connection;
use Flow\Bridge\Symfony\HttpFoundation\Response\FlowStreamedResponse;
use Symfony\Component\Routing\Attribute\Route;

final class OrdersController
{
    public function __construct(
        private readonly Connection $database,
    ) {}

    #[Route('/orders/download', name: 'orders_download')]
    public function download(): FlowStreamedResponse
    {
        $orders = from_dbal_key_set_qb(
            $this->database,
            $this->database->createQueryBuilder()
                ->select('id', 'email', 'country', 'amount', 'created_at')
                ->from('orders'),
            pagination_key_set(pagination_key_asc('id')),
        )->withSchema(schema(
            int_schema('id'),
            str_schema('email'),
            str_schema('country'),
            float_schema('amount'),
            datetime_schema('created_at'),
        ));

        return http_stream_open($orders)
            ->transform(
                mask_columns(['email']),
                with_column('created_at', ref('created_at')->dateFormat('Y-m-d')),
            )
            ->as('orders.csv')
            ->streamedResponse(http_csv_output());
    }
}
```

```text
id,email,country,amount,created_at
1,******,US,0.1,2023-11-14
2,******,DE,0.2,2023-11-14
3,******,FR,0.3,2023-11-14
```

[`from_dbal_key_set_qb`](/documentation/dsl/doctrine/from-dbal-key-set-qb) doesn't ask the database for everything at once. It reads the table page by page, and every page starts where the previous one ended.
The MySQL driver can't tell Flow the type of a column in a query result, so we name the columns and their types with `withSchema`.

## Mask and format the data on the way out

The file goes to somebody outside of our team, so we don't want the emails in it, and nobody needs the time of the order down to the second.

In the controllers written by hand that is three more lines in the loop, in every one of the three controllers:

```php ignore
$order['email'] = '******';
$order['amount'] = (float) $order['amount'];
$order['created_at'] = (new DateTimeImmutable($order['created_at']))->format('Y-m-d');
```

With Flow it is written once, and that is what [`transform`](/documentation/components/bridges/symfony-http-foundation-bridge) is for. [`mask_columns`](/documentation/dsl/core/mask-columns) replaces every email with `******`, and `dateFormat` leaves only the date.
We didn't touch the query, and we didn't write a loop.

## Change the format in one line

Now the JSON and the XML version. With Flow it's the last two lines of the controller:

```php ignore tab="CSV"
->as('orders.csv')
->streamedResponse(http_csv_output());
```

```php ignore tab="JSON"
->as('orders.json')
->streamedResponse(http_json_output());
```

```php ignore tab="XML"
->as('orders.xml')
->streamedResponse(http_xml_output());
```

```php ignore tab="Parquet"
->as('orders.parquet')
->streamedResponse(http_parquet_output());
```

## How much memory does a streamed download take in Symfony

PHP 8.3, `memory_limit=128M`, a million orders in MySQL. Every controller masks the emails and formats the dates, so they all send the same file:

| Format  | `StreamedResponse` by hand | Flow        | Flow with the `flow_php` extension |
|---------|----------------------------|-------------|------------------------------------|
| CSV     | 91Mb, 3.7s                 | 20Mb, 6.5s  | 20Mb, 4.5s                         |
| JSON    | 91Mb, 4.5s                 | 20Mb, 6.7s  | 20Mb, 4.2s                         |
| XML     | 91Mb, 4.9s                 | 20Mb, 7.0s  | 20Mb, 5.3s                         |
| Parquet | -                          | 42Mb, 10.2s | 20Mb, 4.4s                         |

With the extension Flow is as fast as the controllers written by hand for JSON, and close to them for CSV and XML. Without it they are faster.
But they are three different pieces of code, and their memory grows with the table.
The Flow controller is one piece of code with one line changed, and with half of the orders or with all of them it takes the same 20Mb.

> Flow is slower here than it should be. The keyset reader does more work for every row than it needs to, and there is an open issue for it: [flow-php/flow#2748](https://github.com/flow-php/flow/issues/2748).

[flow_php](/documentation/components/extensions/flow-php-ext) is an optional PHP extension. The controller stays the same, without a single change.
