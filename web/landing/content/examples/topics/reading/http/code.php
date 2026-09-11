<?php

declare(strict_types=1);

use Nyholm\Psr7\Factory\Psr17Factory;
use Symfony\Component\HttpClient\{MockHttpClient, Psr18Client};
use Symfony\Component\HttpClient\Response\MockResponse;

use function Flow\ETL\Adapter\Http\{from_http_paginated, http_pagination_page_number, http_request_option_query};
use function Flow\ETL\DSL\{data_frame, int_schema, list_schema, ref, schema, str_schema, to_output};
use function Flow\Types\DSL\{type_integer, type_list, type_string, type_structure};

require __DIR__ . '/vendor/autoload.php';

$product = type_structure(['id' => type_integer(), 'name' => type_string(), 'category' => type_string()]);

$page = static fn (array $items): MockResponse => new MockResponse(
    json_encode(['items' => $items], JSON_THROW_ON_ERROR),
    ['response_headers' => ['Content-Type' => 'application/json']],
);

$client = new Psr18Client(new MockHttpClient([
    $page([
        ['id' => 1, 'name' => 'Keyboard', 'category' => 'peripherals'],
        ['id' => 2, 'name' => 'Mouse', 'category' => 'peripherals'],
    ]),
    $page([
        ['id' => 3, 'name' => 'Monitor', 'category' => 'displays'],
        ['id' => 4, 'name' => 'Webcam', 'category' => 'video'],
    ]),
    $page([]),
]));

data_frame()
    ->read(from_http_paginated(
        $client,
        (new Psr17Factory())
            ->createRequest('GET', 'https://api.example.com/products')
            ->withHeader('Accept', 'application/json'),
        http_pagination_page_number(
            http_request_option_query('page'),
            page_size: 2,
            size_option: http_request_option_query('per_page'),
            records_path: 'items',
        ),
    ))
    // response_body is declared as a string, so its shape must be declared before it can be indexed
    ->withEntry('body', ref('response_body')->jsonDecode()->unpack(schema(
        list_schema('items', type_list($product)),
    )))
    ->withEntry('product', ref('body.items')->expand())
    ->withEntry('product', ref('product')->unpack(schema(
        int_schema('id'),
        str_schema('name'),
        str_schema('category'),
    )))
    ->select('request_uri', 'product.id', 'product.name', 'product.category')
    ->collect()
    ->write(to_output(truncate: false))
    ->run();
