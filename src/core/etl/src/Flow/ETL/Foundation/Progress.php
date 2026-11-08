<?php

declare(strict_types=1);

namespace Flow\ETL\Foundation;

interface Progress
{
    public const int ROWS_PER_CALL = 1000;

    public function advanced(int $rows): void;

    /**
     * Rows read and memory replace this object's previous values.
     * Stage and stage rows are the latest report's; the peak only rises.
     */
    public function reported(
        int $rowsRead,
        string $stage,
        int $stageRows,
        int $memoryBytes,
        int $peakMemoryBytes,
    ): void;
}
