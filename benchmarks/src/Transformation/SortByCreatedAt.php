<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Transformation;

use Flow\ETL\API\DataFrame;
use Flow\ETL\API\Transformation;

use function Flow\ETL\DSL\ref;

final readonly class SortByCreatedAt implements Transformation
{
    public function transform(DataFrame $dataFrame): DataFrame
    {
        return $dataFrame->sortBy([ref('created_at')]);
    }
}
