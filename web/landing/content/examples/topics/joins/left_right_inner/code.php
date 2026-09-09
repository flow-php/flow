<?php

declare(strict_types=1);

use Flow\ETL\Join\Join;

use function Flow\ETL\DSL\{data_frame, equal, from_array, int_schema, join_on, schema, str_schema, to_output};

require __DIR__ . '/vendor/autoload.php';

$left = schema(int_schema('id'), str_schema('name'));
$right = schema(int_schema('id'), str_schema('country'));

// the same two frames, three ways; the row count is the lesson
foreach ([Join::inner, Join::left, Join::right] as $type) {
    echo "\n{$type->value}:\n";

    data_frame()
        ->read(from_array([['id' => 1, 'name' => 'Norbert'], ['id' => 2, 'name' => 'Jane']], $left))
        ->join(
            data_frame()->read(from_array([['id' => 2, 'country' => 'PL'], ['id' => 3, 'country' => 'US']], $right)),
            join_on(equal('id', 'id')),
            $type,
        )
        ->collect()
        ->write(to_output(truncate: false))
        ->run();
}
