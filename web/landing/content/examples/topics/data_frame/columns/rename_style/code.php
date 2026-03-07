<?php

declare(strict_types=1);

use function Flow\ETL\DSL\{data_frame, from_array, rename_style, to_output};
use Flow\ETL\String\StringStyles;

require __DIR__ . '/vendor/autoload.php';

data_frame()
    ->read(from_array([
        ['userId' => 1, 'firstName' => 'Norbert', 'lastName' => 'Orzechowicz'],
        ['userId' => 2, 'firstName' => 'John', 'lastName' => 'Doe'],
    ]))
    ->renameEach(rename_style(StringStyles::SNAKE))
    ->collect()
    ->write(to_output(truncate: false))
    ->run();
