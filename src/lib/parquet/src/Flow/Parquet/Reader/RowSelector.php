<?php

declare(strict_types=1);

namespace Flow\Parquet\Reader;

use Flow\Parquet\Exception\InvalidArgumentException;

final readonly class RowSelector
{
    public function __construct(
        public int $rowCount,
        public bool $skip,
    ) {
        if ($rowCount < 0) {
            throw new InvalidArgumentException('Row selector row count must be 0 or more, got ' . $rowCount);
        }
    }
}
