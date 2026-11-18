---
seo_title: 'Elasticsearch in PHP: index a MySQL or PostgreSQL table and search it'
seo_description: 'LIKE finds nothing when the words change order. How to index a MySQL or PostgreSQL table in Elasticsearch or Meilisearch with PHP, and search it.'
group: Search
problem: 'LIKE finds nothing when the words change order'
proof_label: '100,000 products indexed'
proof_after: '4s'
published: '2026-11-18'
updated: '2026-11-18'
---

# Build a search in PHP: index a database table in Elasticsearch

Let's say we have a shop with a hundred thousand products in MySQL or PostgreSQL, and the search box on the page runs a `LIKE` query.
It worked fine for the first hundred products.

> These scripts need a database and Elasticsearch, so they run only on your machine, not in the playground.

## The table

First we need products to search, so let's create them.

```php ignore tab="MySQL"
<?php

declare(strict_types=1);

$database = new PDO('mysql:host=127.0.0.1;dbname=shop', 'shop', 'secret');
$database->exec('DROP TABLE IF EXISTS products');
$database->exec(
    'CREATE TABLE products (
        id BIGINT PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        description TEXT NOT NULL,
        category VARCHAR(64) NOT NULL,
        price DECIMAL(10, 2) NOT NULL
    )'
);

$rows = 100_000;
$features = ['Wireless', 'Mechanical', 'Compact', 'Ergonomic', 'Gaming', 'Silent', 'Portable', 'Backlit'];
$colors = ['Black', 'White', 'Silver', 'Red', 'Blue'];
$products = ['Keyboard' => 'peripherals', 'Mouse' => 'peripherals', 'Monitor' => 'displays', 'Webcam' => 'video', 'Headset' => 'audio'];
$names = array_keys($products);

$insert = $database->prepare('INSERT INTO products (id, name, description, category, price) VALUES (?, ?, ?, ?, ?)');
$database->beginTransaction();

for ($id = 1; $id <= $rows; $id++) {
    $product = $names[$id % 5];
    $first = $features[$id % 8];
    $second = $features[intdiv($id, 8) % 8];
    $color = $colors[intdiv($id, 5) % 5];

    $insert->execute([
        $id,
        "{$first} {$color} {$product} {$id}",
        "A {$second} {$product} for the office and for home, with two years of warranty.",
        $products[$product],
        ($id % 500) + 9.99,
    ]);
}

$database->commit();

echo "products: {$rows} rows\n";
```

```php ignore tab="PostgreSQL"
<?php

declare(strict_types=1);

$database = new PDO('pgsql:host=127.0.0.1;dbname=shop', 'shop', 'secret');
$database->exec('DROP TABLE IF EXISTS products');
$database->exec(
    'CREATE TABLE products (
        id BIGINT PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        description TEXT NOT NULL,
        category VARCHAR(64) NOT NULL,
        price DECIMAL(10, 2) NOT NULL
    )'
);

$rows = 100_000;
$features = ['Wireless', 'Mechanical', 'Compact', 'Ergonomic', 'Gaming', 'Silent', 'Portable', 'Backlit'];
$colors = ['Black', 'White', 'Silver', 'Red', 'Blue'];
$products = ['Keyboard' => 'peripherals', 'Mouse' => 'peripherals', 'Monitor' => 'displays', 'Webcam' => 'video', 'Headset' => 'audio'];
$names = array_keys($products);

$insert = $database->prepare('INSERT INTO products (id, name, description, category, price) VALUES (?, ?, ?, ?, ?)');
$database->beginTransaction();

for ($id = 1; $id <= $rows; $id++) {
    $product = $names[$id % 5];
    $first = $features[$id % 8];
    $second = $features[intdiv($id, 8) % 8];
    $color = $colors[intdiv($id, 5) % 5];

    $insert->execute([
        $id,
        "{$first} {$color} {$product} {$id}",
        "A {$second} {$product} for the office and for home, with two years of warranty.",
        $products[$product],
        ($id % 500) + 9.99,
    ]);
}

$database->commit();

echo "products: {$rows} rows\n";
```

```text
products: 100000 rows
```

## Search a database table in PHP with LIKE

The first thing that comes to mind is `LIKE` with a percent sign on both sides. In PostgreSQL it's `ILIKE`, because `LIKE` there cares about upper and lower case.

```php ignore tab="MySQL"
<?php

declare(strict_types=1);

$database = new PDO('mysql:host=127.0.0.1;dbname=shop', 'shop', 'secret');
$search = $database->prepare('SELECT id, name, price FROM products WHERE name LIKE ? OR description LIKE ? LIMIT 5');

foreach (['wireless black keyboard', 'black wireless keyboard'] as $query) {
    $search->execute(["%{$query}%", "%{$query}%"]);
    echo $query, ":\n";

    foreach ($search->fetchAll(PDO::FETCH_ASSOC) as $product) {
        printf("  %d %s %.2f\n", $product['id'], $product['name'], $product['price']);
    }
}
```

```php ignore tab="PostgreSQL"
<?php

declare(strict_types=1);

$database = new PDO('pgsql:host=127.0.0.1;dbname=shop', 'shop', 'secret');
$search = $database->prepare('SELECT id, name, price FROM products WHERE name ILIKE ? OR description ILIKE ? LIMIT 5');

foreach (['wireless black keyboard', 'black wireless keyboard'] as $query) {
    $search->execute(["%{$query}%", "%{$query}%"]);
    echo $query, ":\n";

    foreach ($search->fetchAll(PDO::FETCH_ASSOC) as $product) {
        printf("  %d %s %.2f\n", $product['id'], $product['name'], $product['price']);
    }
}
```

```text
wireless black keyboard:
  200 Wireless Black Keyboard 200 209.99
  400 Wireless Black Keyboard 400 409.99
  600 Wireless Black Keyboard 600 109.99
  800 Wireless Black Keyboard 800 309.99
  1000 Wireless Black Keyboard 1000 9.99
black wireless keyboard:
```

## Why LIKE is not a search

The same three words in a different order, and the shop has no keyboards.

`LIKE` doesn't know about words. It looks for one exact piece of text, so the customer has to type the name the way we wrote it.
It also can't tell which product fits better. One has the word in its name, the other only mentions it in the description.

That's what a search engine is for. But a search engine has its own copy of the data, so now there is something to build: read the products from the database, send them to Elasticsearch in bulks, and do it again whenever they change.

## Index a database table in Elasticsearch with Flow

Flow needs PHP 8.3 or newer. It talks to search engines through [SEAL](https://php-cmsig.github.io/search/), a library with one API for Elasticsearch, OpenSearch, Meilisearch, Algolia, Solr, Typesense and a few more.

It reads the table through Doctrine DBAL, or for PostgreSQL through [flow-php/postgresql](/documentation/components/libs/postgresql), Flow's own client with a query builder.

```bash tab="Doctrine MySQL"
composer require flow-php/etl flow-php/etl-adapter-doctrine flow-php/etl-adapter-seal cmsig/seal-elasticsearch-adapter
```

```bash tab="Doctrine PostgreSQL"
composer require flow-php/etl flow-php/etl-adapter-doctrine flow-php/etl-adapter-seal cmsig/seal-elasticsearch-adapter
```

```bash tab="Flow PostgreSQL"
composer require flow-php/etl flow-php/etl-adapter-postgresql flow-php/etl-adapter-seal cmsig/seal-elasticsearch-adapter
```

And this is the whole indexer:

```php ignore tab="Doctrine MySQL"
<?php

declare(strict_types=1);

use function Flow\ETL\Adapter\Doctrine\{from_dbal_key_set_qb, pagination_key_asc, pagination_key_set};
use function Flow\ETL\Adapter\Seal\{to_seal_schema, to_seal_upsert};
use function Flow\ETL\DSL\{data_frame, float_schema, schema, str_schema};
use CmsIg\Seal\Adapter\Elasticsearch\ElasticsearchAdapter;
use CmsIg\Seal\Engine;
use Doctrine\DBAL\DriverManager;
use Elastic\Elasticsearch\ClientBuilder;
use Flow\ETL\Adapter\Seal\Values\Schema\SealMetadata;

require __DIR__ . '/vendor/autoload.php';

$database = DriverManager::getConnection([
    'driver' => 'pdo_mysql',
    'host' => '127.0.0.1',
    'dbname' => 'shop',
    'user' => 'shop',
    'password' => 'secret',
]);

$products = schema(
    str_schema('id'),
    str_schema('name'),
    str_schema('description'),
    str_schema('category', metadata: SealMetadata::filterable()),
    float_schema('price'),
);

$engine = new Engine(
    new ElasticsearchAdapter(ClientBuilder::create()->setHosts(['http://127.0.0.1:9200'])->build()),
    to_seal_schema($products, index_name: 'products', identifier: 'id'),
);

if (!$engine->existIndex('products')) {
    $engine->createIndex('products');
}

data_frame()
    ->read(
        from_dbal_key_set_qb(
            $database,
            $database->createQueryBuilder()
                ->select('id', 'name', 'description', 'category', 'price')
                ->from('products'),
            pagination_key_set(pagination_key_asc('id')),
        )->withSchema($products)
    )
    ->write(to_seal_upsert($engine, 'products')->withBulkSize(1000))
    ->run();
```

```php ignore tab="Doctrine PostgreSQL"
<?php

declare(strict_types=1);

use function Flow\ETL\Adapter\Doctrine\{from_dbal_key_set_qb, pagination_key_asc, pagination_key_set};
use function Flow\ETL\Adapter\Seal\{to_seal_schema, to_seal_upsert};
use function Flow\ETL\DSL\{data_frame, float_schema, schema, str_schema};
use CmsIg\Seal\Adapter\Elasticsearch\ElasticsearchAdapter;
use CmsIg\Seal\Engine;
use Doctrine\DBAL\DriverManager;
use Elastic\Elasticsearch\ClientBuilder;
use Flow\ETL\Adapter\Seal\Values\Schema\SealMetadata;

require __DIR__ . '/vendor/autoload.php';

$database = DriverManager::getConnection([
    'driver' => 'pdo_pgsql',
    'host' => '127.0.0.1',
    'dbname' => 'shop',
    'user' => 'shop',
    'password' => 'secret',
]);

$products = schema(
    str_schema('id'),
    str_schema('name'),
    str_schema('description'),
    str_schema('category', metadata: SealMetadata::filterable()),
    float_schema('price'),
);

$engine = new Engine(
    new ElasticsearchAdapter(ClientBuilder::create()->setHosts(['http://127.0.0.1:9200'])->build()),
    to_seal_schema($products, index_name: 'products', identifier: 'id'),
);

if (!$engine->existIndex('products')) {
    $engine->createIndex('products');
}

data_frame()
    ->read(
        from_dbal_key_set_qb(
            $database,
            $database->createQueryBuilder()
                ->select('id', 'name', 'description', 'category', 'price')
                ->from('products'),
            pagination_key_set(pagination_key_asc('id')),
        )->withSchema($products)
    )
    ->write(to_seal_upsert($engine, 'products')->withBulkSize(1000))
    ->run();
```

```php ignore tab="Flow PostgreSQL"
<?php

declare(strict_types=1);

use function Flow\ETL\Adapter\PostgreSql\{from_pgsql_key_set, pgsql_pagination_key_asc, pgsql_pagination_key_set};
use function Flow\ETL\Adapter\Seal\{to_seal_schema, to_seal_upsert};
use function Flow\ETL\DSL\{data_frame, float_schema, schema, str_schema};
use function Flow\PostgreSql\DSL\{col, pgsql_client, pgsql_connection_dsn, select, table};
use CmsIg\Seal\Adapter\Elasticsearch\ElasticsearchAdapter;
use CmsIg\Seal\Engine;
use Elastic\Elasticsearch\ClientBuilder;
use Flow\ETL\Adapter\Seal\Values\Schema\SealMetadata;

require __DIR__ . '/vendor/autoload.php';

$client = pgsql_client(pgsql_connection_dsn('postgres://shop:secret@127.0.0.1:5432/shop'));

$products = schema(
    str_schema('id'),
    str_schema('name'),
    str_schema('description'),
    str_schema('category', metadata: SealMetadata::filterable()),
    float_schema('price'),
);

$engine = new Engine(
    new ElasticsearchAdapter(ClientBuilder::create()->setHosts(['http://127.0.0.1:9200'])->build()),
    to_seal_schema($products, index_name: 'products', identifier: 'id'),
);

if (!$engine->existIndex('products')) {
    $engine->createIndex('products');
}

data_frame()
    ->read(
        from_pgsql_key_set(
            $client,
            select(col('id'), col('name'), col('description'), col('category'), col('price'))->from(table('products')),
            pgsql_pagination_key_set(pgsql_pagination_key_asc('id')),
        )->withSchema($products)
    )
    ->write(to_seal_upsert($engine, 'products')->withBulkSize(1000))
    ->run();
```

A hundred thousand products are in the index after three to four seconds, from either database.

The schema is written once and used twice. [`to_seal_schema`](/documentation/components/adapters/seal) makes the index out of it: text columns can be searched, `price` can be sorted and filtered, and `category` can be filtered because we said so.
The reader uses the same schema to give the columns their types.

The keyset reader takes the table page by page, so the products are never in memory all at once.
[`to_seal_upsert`](/documentation/components/adapters/seal) sends them in bulks of a thousand.

## Search the index

Now the search box asks Elasticsearch, with a filter on the category. This script is the same for every database:

```php ignore
<?php

declare(strict_types=1);

use function Flow\ETL\Adapter\Seal\to_seal_schema;
use function Flow\ETL\DSL\{float_schema, schema, str_schema};
use CmsIg\Seal\Adapter\Elasticsearch\ElasticsearchAdapter;
use CmsIg\Seal\Engine;
use CmsIg\Seal\Search\Condition\{EqualCondition, SearchCondition};
use Elastic\Elasticsearch\ClientBuilder;
use Flow\ETL\Adapter\Seal\Values\Schema\SealMetadata;

require __DIR__ . '/vendor/autoload.php';

$engine = new Engine(
    new ElasticsearchAdapter(ClientBuilder::create()->setHosts(['http://127.0.0.1:9200'])->build()),
    to_seal_schema(
        schema(
            str_schema('id'),
            str_schema('name'),
            str_schema('description'),
            str_schema('category', metadata: SealMetadata::filterable()),
            float_schema('price'),
        ),
        index_name: 'products',
        identifier: 'id',
    ),
);

foreach (['wireless black keyboard', 'black wireless keyboard'] as $query) {
    $result = $engine->createSearchBuilder('products')
        ->addFilter(new SearchCondition($query))
        ->addFilter(new EqualCondition('category', 'peripherals'))
        ->limit(5)
        ->getResult();

    echo $query, ":\n";

    foreach ($result as $product) {
        printf("  %s %s %.2f\n", $product['id'], $product['name'], $product['price']);
    }
}
```

```text
wireless black keyboard:
  200 Wireless Black Keyboard 200 209.99
  400 Wireless Black Keyboard 400 409.99
  600 Wireless Black Keyboard 600 109.99
  800 Wireless Black Keyboard 800 309.99
  1000 Wireless Black Keyboard 1000 9.99
black wireless keyboard:
  200 Wireless Black Keyboard 200 209.99
  400 Wireless Black Keyboard 400 409.99
  600 Wireless Black Keyboard 600 109.99
  800 Wireless Black Keyboard 800 309.99
  1000 Wireless Black Keyboard 1000 9.99
```

Both orders of the words find the keyboards.

## Change the search engine

Elasticsearch is not the only choice, and with SEAL it's not a decision for life.
To move the same search to Meilisearch, install its adapter:

```bash tab="Elasticsearch"
composer require cmsig/seal-elasticsearch-adapter
```

```bash tab="Meilisearch"
composer require cmsig/seal-meilisearch-adapter
```

And change how the engine gets its adapter, in both scripts:

```php ignore tab="Elasticsearch"
use CmsIg\Seal\Adapter\Elasticsearch\ElasticsearchAdapter;
use Elastic\Elasticsearch\ClientBuilder;

new ElasticsearchAdapter(ClientBuilder::create()->setHosts(['http://127.0.0.1:9200'])->build())
```

```php ignore tab="Meilisearch"
use CmsIg\Seal\Adapter\Meilisearch\MeilisearchAdapter;
use Meilisearch\Client;

new MeilisearchAdapter(new Client('http://127.0.0.1:7700', 'masterKey'))
```

The schema, the reader, `to_seal_upsert` and the search stay as they are.

## Keep the index up to date

Products change, so the index has to follow.

The script above is already an upsert. A product that is in the index gets replaced, and a new one gets added, so we can run it every night as it is.
To send only what changed, add a `where` to the query, for example on an `updated_at` column.

Products that are gone from the table need one more step. [`to_seal_delete`](/documentation/components/adapters/seal) takes their ids and removes them from the index:

```php ignore
use function Flow\ETL\Adapter\Seal\to_seal_delete;
use function Flow\ETL\DSL\{data_frame, from_array};

data_frame()
    ->read(from_array([['id' => '200'], ['id' => '400']]))
    ->write(to_seal_delete($engine, 'products'))
    ->run();
```
