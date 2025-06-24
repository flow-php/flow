<?php

declare(strict_types=1);

use function Flow\ETL\DSL\{data_frame,
    from_rows,
    lit,
    match_cases,
    match_condition,
    ref,
    row,
    rows,
    string_entry,
    to_stream
};
use function Flow\Types\DSL\type_integer;

require __DIR__ . '/vendor/autoload.php';

data_frame()
    ->read(
        from_rows(
            rows(
                row(string_entry('string', 'string-with-dashes')),
                row(string_entry('string', '123')),
                row(string_entry('string', '14%')),
                row(string_entry('string', '+14')),
                row(string_entry('string', ''))
            )
        )
    )
    ->withEntry(
        'string',
        match_cases(
            [
                match_condition(ref('string')->contains('-'), ref('string')->strReplace('-', ' ')),
                match_condition(ref('string')->call('is_numeric'), ref('string')->cast(type_integer())),
                match_condition(ref('string')->endsWith('%'), ref('string')->strReplace('%', '')->cast(type_integer())),
                match_condition(ref('string')->startsWith('+'), ref('string')->strReplace('+', '')->cast(type_integer())),
            ],
            default: lit('DEFAULT')
        )
    )
    ->write(to_stream(__DIR__ . '/output.txt', truncate: false))
    ->run();
