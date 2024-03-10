<?php

declare(strict_types=1);

use function Flow\ETL\DSL\{bool_schema, df, from_array, int_schema, schema, str_schema, to_output};
use Flow\ETL\Loader\StreamLoader\Output;
use Flow\ETL\Row\Schema\Metadata;

require __DIR__ . '/../../../autoload.php';

$schema = schema(
    int_schema('id', $nullable = false),
    str_schema('name', $nullable = true),
    bool_schema('active', $nullable = false, Metadata::empty()->add('key', 'value')),
);

df()
    ->read(from_array([
        ['id' => 1, 'name' => 'Product 1', 'active' => true],
        ['id' => 2, 'name' => 'Product 2', 'active' => false],
        ['id' => 3, 'name' => 'Product 3', 'active' => true],
    ]))
    ->validate($schema)
    ->write(to_output(false, Output::rows_and_schema))
    ->run();
