<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\IO\Extractor;
use Flow\ETL\IO\Extractor\ExtractorFactory;
use Flow\ETL\Values\Rows;

final readonly class StaticExtractorFactory implements ExtractorFactory
{
    public function __construct(
        private Extractor $extractor,
    ) {}

    public function from(Rows $rows): Extractor
    {
        return $this->extractor;
    }
}
