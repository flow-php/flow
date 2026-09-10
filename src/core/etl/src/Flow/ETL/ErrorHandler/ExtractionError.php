<?php

declare(strict_types=1);

namespace Flow\ETL\ErrorHandler;

use Flow\ETL\Extractor;
use Throwable;

final readonly class ExtractionError
{
    public function __construct(
        public Throwable $cause,
        public Extractor $extractor,
    ) {}
}
