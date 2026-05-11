<?php

declare(strict_types=1);

namespace Flow\ETL\Transformation;

use Flow\ETL\DataFrame;
use Flow\ETL\Row;
use Flow\ETL\Transformation;
use Flow\ETL\Transformation\AddRowIndex\StartFrom;

use function Flow\ETL\DSL\int_entry;

final readonly class AddRowIndex implements Transformation
{
    public function __construct(
        private string $indexColumn = 'index',
        private StartFrom $startFrom = StartFrom::ZERO,
    ) {}

    public function transform(DataFrame $dataFrame): DataFrame
    {
        $index = $this->startFrom === StartFrom::ZERO ? 0 : 1;

        return $dataFrame->map(function (Row $row) use (&$index) {
            $row = $row->add(int_entry($this->indexColumn, $index));
            $index++;

            return $row;
        });
    }
}
