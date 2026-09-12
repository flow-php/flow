<?php

declare(strict_types=1);

use function Flow\ETL\DSL\{array_expand, data_frame, from_array, int_schema, list_schema, map_schema, ref, schema, structure, to_output};
use function Flow\Types\DSL\{type_integer, type_list, type_map, type_string};

require __DIR__ . '/vendor/autoload.php';

data_frame()
    ->read(from_array(
        [['id' => 1, 'array' => ['a' => 1, 'b' => 2, 'c' => 3]]],
        // array_expand() derives its own type from the element type, so a bare json column is not enough
        schema(int_schema('id'), map_schema('array', type_map(type_string(), type_integer()))),
    ))
    ->withEntry('expanded', array_expand(ref('array')))
    ->write(to_output(truncate: false))
    ->run();

data_frame()
    ->read(from_array(
        [['id' => 1, 'tags' => ['a', 'b'], 'scores' => [10, 20, 30]]],
        schema(int_schema('id'), list_schema('tags', type_list(type_string())), list_schema('scores', type_list(type_integer()))),
    ))
    // expands in one expression are zipped to the longest list, a shorter one gives null
    ->withEntry('pair', structure(['tag' => ref('tags')->expand(), 'score' => ref('scores')->expand()]))
    ->drop('tags', 'scores')
    ->write(to_output(truncate: false))
    ->run();
