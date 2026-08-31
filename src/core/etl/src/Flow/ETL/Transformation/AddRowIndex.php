<?php

declare(strict_types=1);

namespace Flow\ETL\Transformation;

use Flow\ETL\DataFrame;
use Flow\ETL\Transformation;
use Flow\ETL\Transformation\AddRowIndex\StartFrom;
use Flow\ETL\Transformer\AddRowIndexTransformer;

final readonly class AddRowIndex implements Transformation
{
    public function __construct(
        private string $indexColumn = 'index',
        private StartFrom $startFrom = StartFrom::ZERO,
    ) {}

    public function transform(DataFrame $dataFrame): DataFrame
    {
        return $dataFrame->rows(new AddRowIndexTransformer($this->indexColumn, $this->startFrom));
    }
}
