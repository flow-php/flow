<?php

declare(strict_types=1);

use function Flow\ETL\Adapter\Elasticsearch\{entry_id_factory, es_hits_to_rows, from_es, to_es_bulk_index};
use function Flow\ETL\DSL\{data_frame, from_array, to_output};
use Symfony\Component\Dotenv\Dotenv;

require __DIR__ . '/vendor/autoload.php';

if (!\file_exists(__DIR__ . '/.env')) {
    print 'Example skipped. Please create .env file with Azure Storage Account credentials.' . PHP_EOL;

    return;
}

$dotenv = new Dotenv();
$dotenv->load(__DIR__ . '/.env');

$elasticsearchUrl = $_ENV['ELASTICSEARCH_URL'];

if (!\is_string($elasticsearchUrl)) {
    print 'Example skipped. ELASTICSEARCH_URL must be a string.' . PHP_EOL;

    return;
}

data_frame()
    ->read(from_array([
        ['id' => 1, 'text' => 'lorem ipsum'],
        ['id' => 2, 'text' => 'lorem ipsum'],
        ['id' => 3, 'text' => 'lorem ipsum'],
        ['id' => 4, 'text' => 'lorem ipsum'],
        ['id' => 5, 'text' => 'lorem ipsum'],
        ['id' => 6, 'text' => 'lorem ipsum'],
    ]))
    ->write(
        to_es_bulk_index(
            [
                'hosts' => [$elasticsearchUrl],
            ],
            $index = 'test_index',
            entry_id_factory('id')
        )
    )
    ->run();

data_frame()
    ->read(from_es(
        [
            'hosts' => [$elasticsearchUrl],
        ],
        [
            'index' => $index,
            'body' => [
                'query' => [
                    'match_all' => ['boost' => 1.0],
                ],
            ],
        ]
    ))
    ->write(to_output(truncate: false))
    ->with(es_hits_to_rows())
    ->write(to_output(truncate: false))
    ->run();
