<?php

declare(strict_types=1);

use Flow\ETL\Values\Column\PhpBackend;

use function Flow\ETL\DSL\concat;
use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_sequence_number;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\split;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;

require $argv[1];

$frame = df(config_builder()->backend(new PhpBackend()))->read(from_sequence_number('id', 1, (int) $argv[2]));

if ($argv[3] === 'list') {
    $frame = $frame->withColumn(
        'list',
        split(concat(ref('id')->cast('string'), lit(',2,3,4,5,6,7,8')), ',')->cast(type_list(type_integer())),
    );
}

echo $frame->sortBy(ref('id')->desc())->count();
