<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\IO\Loader;

use DateTimeInterface;
use Flow\ETL\Adapter\CSV\IO\CSVDialect;

final readonly class CSVWriteOptions
{
    public function __construct(
        public string $separator = ',',
        public string $enclosure = '"',
        public string $escape = '\\',
        public string $newLineSeparator = PHP_EOL,
        public string $dateTimeFormat = DateTimeInterface::ATOM,
        public string $dateFormat = 'Y-m-d',
    ) {
        CSVDialect::assertParts($separator, $enclosure, $escape);
    }
}
