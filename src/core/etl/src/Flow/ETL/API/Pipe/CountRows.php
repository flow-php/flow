<?php

declare(strict_types=1);

namespace Flow\ETL\API\Pipe;

use Flow\ETL\API\DataFrame;

final readonly class CountRows
{
    public function __invoke(DataFrame $dataFrame): int
    {
        return $dataFrame->count();
    }
}
