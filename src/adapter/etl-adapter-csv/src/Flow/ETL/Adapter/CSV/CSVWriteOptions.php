<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV;

use DateTimeInterface;
use Flow\ETL\Exception\InvalidArgumentException;

use function strlen;

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
        if (strlen($separator) !== 1) {
            throw new InvalidArgumentException('Separator must be a single character');
        }

        if (strlen($enclosure) !== 1) {
            throw new InvalidArgumentException('Enclosure must be a single character');
        }

        if (strlen($escape) > 1) {
            throw new InvalidArgumentException('Escape must be empty or a single character');
        }
    }
}
