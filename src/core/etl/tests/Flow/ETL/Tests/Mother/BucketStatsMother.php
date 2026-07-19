<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Mother;

use Flow\ETL\Bucketing\BucketStats;
use Flow\ETL\Row\References;

final class BucketStatsMother
{
    /**
     * @param array<string, mixed> $min
     * @param array<string, mixed> $max
     * @param array<string, int> $nullCounts
     */
    public static function with(
        int $rowsCount = 0,
        int $chunksCount = 1,
        array $min = [],
        array $max = [],
        array $nullCounts = [],
        ?int $distinctEstimate = null,
        bool $distinctExact = false,
        ?References $sortedBy = null,
    ): BucketStats {
        return new BucketStats(
            $rowsCount,
            $chunksCount,
            $min,
            $max,
            $nullCounts,
            $distinctEstimate,
            $distinctExact,
            $sortedBy,
        );
    }
}
