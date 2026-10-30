<?php

declare(strict_types=1);

namespace Flow\ETL\API\Transformation;

use Flow\ETL\API\DataFrame;
use Flow\ETL\API\Transformation;
use Flow\ETL\Operators\Transformer\AddRowIndex\StartFrom;
use Flow\ETL\Operators\Transformer\AddRowIndexTransformer;

final readonly class AddRowIndex implements Transformation
{
    public function __construct(
        private string $indexColumn = 'index',
        private StartFrom $startFrom = StartFrom::ZERO,
    ) {}

    public function transform(DataFrame $dataFrame): DataFrame
    {
        return $dataFrame->with(new AddRowIndexTransformer($this->indexColumn, $this->startFrom));
    }
}
