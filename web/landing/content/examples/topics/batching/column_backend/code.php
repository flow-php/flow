<?php

declare(strict_types=1);

use Flow\ETL\Column\PhpBackend;

use function Flow\ETL\DSL\{config_builder, data_frame, from_array, lit, ref};

require __DIR__ . '/vendor/autoload.php';

$rows = data_frame(config_builder()->backend(new PhpBackend()))
    ->read(from_array([['id' => 1], ['id' => 2], ['id' => 3]]))
    ->withEntry('double', ref('id')->multiply(lit(2)))
    ->fetch();

foreach (['id', 'double'] as $name) {
    $column = $rows->column($name);

    echo $name . ': ' . $column::class . ' ' . json_encode($column->values()) . "\n";
}
