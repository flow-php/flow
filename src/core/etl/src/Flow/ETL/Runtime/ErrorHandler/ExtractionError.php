<?php

declare(strict_types=1);

namespace Flow\ETL\Runtime\ErrorHandler;

use Flow\ETL\IO\Extractor;
use Flow\ETL\IO\Extractor\Reader;
use Throwable;

final readonly class ExtractionError
{
    public function __construct(
        public Throwable $cause,
        public Extractor|Reader $extractor,
    ) {}
}
