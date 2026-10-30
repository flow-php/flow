<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Joining;

use Flow\ETL\IO\Extractor;
use Flow\ETL\IO\Extractor\ExtractorFactory;
use Flow\ETL\Values\Rows;

use function Flow\ETL\Adapter\Parquet\from_parquet;

final readonly class SellersExtractorFactory implements ExtractorFactory
{
    public function __construct(
        private string $sellersPath,
    ) {}

    public function from(Rows $rows): Extractor
    {
        return from_parquet($this->sellersPath);
    }
}
