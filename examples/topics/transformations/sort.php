<?php

declare(strict_types=1);

use function Flow\ETL\DSL\ref;
use Flow\ETL\DSL\From;
use Flow\ETL\DSL\To;
use Flow\ETL\Flow;

require __DIR__ . '/../../bootstrap.php';

return (new Flow())
    ->read(From::sequence_number('id', 0, 10))
    ->sortBy(ref('id')->desc())
    ->write(To::output(false));
