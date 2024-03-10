<?php

declare(strict_types=1);

use function Flow\ETL\DSL\{array_entry, array_expand, df, from_rows, int_entry, ref, row, rows, to_output};

// flow.phar run examples/topics/phar/data_frame/code.php
// when executing data processing pipeline through phar make sure to not use any trigger, like ->run();
// this is handled by the phar internally.
return df()
    ->read(from_rows(rows(
        row(int_entry('id', 1), array_entry('array', ['a' => 1, 'b' => 2, 'c' => 3])),
    )))
    ->write(to_output(false))
    ->withEntry('expanded', array_expand(ref('array')))
    ->write(to_output(false));
