<?php

declare(strict_types=1);

namespace Flow\ETL\IO\Extractor\File;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;

final readonly class SkippedRows
{
    /**
     * @throws InvalidArgumentException when $count is negative
     */
    public function __construct(
        public int $count,
    ) {
        if ($count < 0) {
            throw new InvalidArgumentException('Skipped rows cannot be negative, given: ' . $count);
        }
    }
}
