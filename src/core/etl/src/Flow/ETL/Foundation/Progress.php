<?php

declare(strict_types=1);

namespace Flow\ETL\Foundation;

interface Progress
{
    public const int ROWS_PER_CALL = 1000;

    public function advanced(int $rows): void;

    /**
     * Totals of the work done in other processes; each call replaces this object's previous totals.
     */
    public function reported(int $rows, string $stage, int $stageRows, int $memoryBytes, int $peakMemoryBytes): void;
}
