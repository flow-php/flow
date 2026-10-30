<?php

declare(strict_types=1);

namespace Flow\ETL\API\Transformation;

use Flow\ETL\API\DataFrame;
use Flow\ETL\API\Transformation;
use Flow\ETL\Values\BatchSize as RowsBatchSize;

final readonly class BatchSize implements Transformation
{
    public function __construct(
        private RowsBatchSize $size,
    ) {}

    public function transform(DataFrame $dataFrame): DataFrame
    {
        return $dataFrame->batchSize($this->size->size);
    }
}
