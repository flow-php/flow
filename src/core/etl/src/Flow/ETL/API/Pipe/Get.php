<?php

declare(strict_types=1);

namespace Flow\ETL\API\Pipe;

use Flow\ETL\API\DataFrame;
use Flow\ETL\Values\Rows;
use Generator;

final readonly class Get
{
    /**
     * @return Generator<Rows>
     */
    public function __invoke(DataFrame $dataFrame): Generator
    {
        return $dataFrame->get();
    }
}
