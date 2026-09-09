<?php

declare(strict_types=1);

use function Flow\ETL\DSL\{data_frame, from_array, int_schema, schema, schema_evolving_validator, schema_strict_validator, str_schema, to_output};

require __DIR__ . '/vendor/autoload.php';

$rows = [
    ['id' => 1, 'name' => 'Norbert', 'extra' => 'unexpected'],
];
$declared = schema(int_schema('id', nullable: true), str_schema('name', nullable: true));

// evolving: an undeclared column is allowed through
data_frame()
    ->read(from_array($rows))
    ->match($declared, schema_evolving_validator())
    ->collect()
    ->write(to_output(truncate: false))
    ->run();

// strict: the same column is a failure
try {
    data_frame()
        ->read(from_array($rows))
        ->match($declared, schema_strict_validator())
        ->collect()
        ->write(to_output(truncate: false))
        ->run();
} catch (Throwable $e) {
    echo "\nstrict rejects it: " . explode("\n", $e->getMessage())[0] . "\n";
}
