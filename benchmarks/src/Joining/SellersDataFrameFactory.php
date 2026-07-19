<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Joining;

use Flow\ETL\DataFrame;
use Flow\ETL\DataFrameFactory;
use Flow\ETL\Rows;

use function Flow\ETL\Adapter\Parquet\from_parquet;
use function Flow\ETL\DSL\data_frame;

final readonly class SellersDataFrameFactory implements DataFrameFactory
{
    public function __construct(
        private string $sellersPath,
    ) {}

    public function from(Rows $rows): DataFrame
    {
        return data_frame()->read(from_parquet($this->sellersPath));
    }
}
