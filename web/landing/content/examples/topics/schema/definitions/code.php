<?php

declare(strict_types=1);

use function Flow\ETL\DSL\{integer_schema, schema, schema_to_ascii, string_schema, structure_schema};
use function Flow\Types\DSL\{type_float, type_string, type_structure};

require __DIR__ . '/vendor/autoload.php';

echo schema_to_ascii(schema(
    integer_schema('id'),
    string_schema('name', nullable: true),
    structure_schema('address', type_structure([
        'city' => type_string(),
        'lat' => type_float(),
        'lon' => type_float(),
    ])),
));
