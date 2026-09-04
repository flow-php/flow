<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV;

final readonly class CSVDialect
{
    public function __construct(
        public string $separator,
        public string $enclosure,
        public string $escape,
    ) {}
}
