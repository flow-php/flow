<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\DataFrame;
use Flow\ETL\DataFrameFactory;
use Flow\ETL\Rows;

final readonly class StaticDataFrameFactory implements DataFrameFactory
{
    public function __construct(
        private DataFrame $dataFrame,
    ) {}

    public function from(Rows $rows): DataFrame
    {
        return $this->dataFrame;
    }
}
