<?php

declare(strict_types=1);

namespace Flow\ETL\Runtime\Heartbeat;

use Flow\ETL\Foundation\Progress;

final class HeartbeatStage implements Progress
{
    private int $reportedMemoryBytes = 0;

    private int $reportedRows = 0;

    private int $rows = 0;

    public function __construct(
        private readonly string $name,
        private readonly HeartbeatThrottle $throttle,
    ) {}

    public function advanced(int $rows): void
    {
        $this->rows += $rows;
        $this->throttle->stageAdvanced($this->name, $this->rows);
    }

    public function reported(int $rows, string $stage, int $stageRows, int $memoryBytes, int $peakMemoryBytes): void
    {
        $addedRows = $rows - $this->reportedRows;
        $addedMemoryBytes = $memoryBytes - $this->reportedMemoryBytes;
        $this->reportedRows = $rows;
        $this->reportedMemoryBytes = $memoryBytes;

        $this->throttle->othersReported($addedRows, $stage, $stageRows, $addedMemoryBytes, $peakMemoryBytes);
    }
}
