<?php

declare(strict_types=1);

use function Flow\ETL\DSL\{data_frame,
    from_array,
    lit,
    ref,
    to_stream};
use function Flow\Types\DSL\{type_integer, type_list};

require __DIR__ . '/../../../../vendor/autoload.php';

(data_frame())
    ->read(
        from_array(
            [
                ['integers' => '1,2,3'],
                ['integers' => '5,7'],
                ['integers' => '0,2,4'],
            ]
        )
    )
    ->withEntry(
        'integers',
        ref('integers')->call(lit('explode'), ['separator' => ','], refAlias: 'string', returnType: type_list(type_integer()))
    )
    ->write(to_stream(__DIR__ . '/output.txt', truncate: false))
    ->run();
