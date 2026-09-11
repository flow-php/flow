<?php

declare(strict_types=1);

use function Flow\ETL\DSL\{
    data_frame,
    from_array,
    lit,
    match_cases,
    match_condition,
    ref,
    schema,
    str_schema,
    to_output
};
use function Flow\Types\DSL\{type_boolean, type_integer};

require __DIR__ . '/vendor/autoload.php';

data_frame()
    ->read(from_array(
        [
            ['string' => 'string-with-dashes'],
            ['string' => '123'],
            ['string' => '14%'],
            ['string' => '+14'],
            ['string' => ''],
        ],
        schema(str_schema('string')),
    ))
    ->withEntry(
        'string',
        match_cases(
            [
                match_condition(ref('string')->contains('-'), ref('string')->strReplace('-', ' ')),
                match_condition(ref('string')->call(lit('is_numeric'), type_boolean()), ref('string')->cast(type_integer())),
                match_condition(ref('string')->endsWith('%'), ref('string')->strReplace('%', '')->cast(type_integer())),
                match_condition(ref('string')->startsWith('+'), ref('string')->strReplace('+', '')->cast(type_integer())),
            ],
            default: lit('DEFAULT')
        )
    )
    ->write(to_output(truncate: false))
    ->run();
