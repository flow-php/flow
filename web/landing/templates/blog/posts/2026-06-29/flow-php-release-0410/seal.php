<?php

use CmsIg\Seal\Adapter\Elasticsearch\ElasticsearchAdapter;
use CmsIg\Seal\Engine;

use function Flow\ETL\Adapter\Seal\to_seal_schema;
use function Flow\ETL\DSL\{int_schema, schema, str_schema};
use function Flow\ETL\Adapter\Seal\to_seal_upsert;
use function Flow\ETL\Adapter\CSV\from_csv;
use function Flow\ETL\DSL\data_frame;

// @var \Elastic\Elasticsearch\Client $client
$client = require 'elastic_client.php';
$schema = to_seal_schema(
    schema(
        str_schema('id'),
        str_schema('name'),
        int_schema('age'),
    ),
    index_name: 'users',
    identifier: 'id',
);

$engine = new Engine(new ElasticsearchAdapter($client), $schema);
$engine->createIndex('users');

data_frame()
    ->read(from_csv(__DIR__ . '/users.csv'))
    ->write(to_seal_upsert($engine, 'users'))
    ->run();