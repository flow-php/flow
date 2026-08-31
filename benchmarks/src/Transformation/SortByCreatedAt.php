<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Transformation;

use Flow\ETL\DataFrame;
use Flow\ETL\Transformation;

use function Flow\ETL\DSL\ref;

final readonly class SortByCreatedAt implements Transformation
{
    public function transform(DataFrame $dataFrame): DataFrame
    {
        return $dataFrame->sortBy([ref('created_at')]);
    }
}
