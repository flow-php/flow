<?php

declare(strict_types=1);

use function Flow\ETL\DSL\{concat_ws, data_frame, from_array, lit, ref, to_output};
use Flow\ETL\Transformation;

require __DIR__ . '/vendor/autoload.php';

data_frame()
    ->read(
        from_array([
            ['id' => 1, 'first_name' => 'John', 'last_name' => 'Doe'],
            ['id' => 2, 'first_name' => 'Jane', 'last_name' => 'Smith'],
            ['id' => 3, 'first_name' => 'Bob', 'last_name' => 'Johnson'],
            ['id' => 4, 'first_name' => 'Alice', 'last_name' => 'Williams'],
        ])
    )
    ->with(
        /**
         *  Create new column "name" by concatenating "first_name" and "last_name" columns.
         */
        new class implements Transformation {
            public function transform(Flow\ETL\DataFrame $dataFrame) : Flow\ETL\DataFrame
            {
                return $dataFrame->withEntry('name', concat_ws(lit(' '), ref('first_name'), ref('last_name')));
            }
        }
    )
    ->collect()
    ->write(to_output(truncate: false))
    ->run();
