<?php

declare(strict_types=1);

namespace Flow\Parquet\Data;

final readonly class DeltaHeader
{
    public function __construct(
        public int $blockSize,
        public int $miniblockCount,
        public int $totalValues,
        public int $firstValue,
    ) {
    }
}
