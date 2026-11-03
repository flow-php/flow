<?php

declare(strict_types=1);

namespace Flow\ETL\API\Pipe;

use Flow\ETL\API\DataFrame;
use Generator;

final readonly class GetEachAsArray
{
    /**
     * @return Generator<array<mixed>>
     */
    public function __invoke(DataFrame $dataFrame): Generator
    {
        return $dataFrame->getEachAsArray();
    }
}
