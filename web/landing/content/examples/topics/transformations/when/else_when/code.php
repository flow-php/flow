<?php

declare(strict_types=1);

use function Flow\ETL\DSL\{data_frame, from_array, lit, ref, to_output, when};

require __DIR__ . '/vendor/autoload.php';

data_frame()
    ->read(from_array([
        ['id' => 1, 'score' => 95, 'email' => 'user01@flow-php.com'],
        ['id' => 2, 'score' => 75, 'email' => 'user02@flow-php.com'],
        ['id' => 3, 'score' => 55, 'email' => 'user03@flow-php.com'],
        ['id' => 4, 'score' => 30, 'email' => 'user04@flow-php.com'],
    ]))
    ->collect()
    ->withEntry(
        'grade',
        when(
            ref('score')->greaterThanEqual(lit(90)),
            lit('A'),
            when(
                ref('score')->greaterThanEqual(lit(70)),
                lit('B'),
                when(
                    ref('score')->greaterThanEqual(lit(50)),
                    lit('C'),
                    lit('F')
                )
            )
        )
    )
    ->write(to_output(truncate: false))
    ->run();
