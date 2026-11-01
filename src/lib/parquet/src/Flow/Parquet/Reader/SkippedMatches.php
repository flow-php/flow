<?php

declare(strict_types=1);

namespace Flow\Parquet\Reader;

use Flow\Parquet\Exception\InvalidArgumentException;

final readonly class SkippedMatches
{
    /**
     * @throws InvalidArgumentException when $count is negative
     */
    public function __construct(
        public int $count,
    ) {
        if ($count < 0) {
            throw new InvalidArgumentException('Skipped matches cannot be negative, given: ' . $count);
        }
    }
}
