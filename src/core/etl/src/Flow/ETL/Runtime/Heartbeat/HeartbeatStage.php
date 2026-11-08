<?php

declare(strict_types=1);

namespace Flow\ETL\Runtime\Heartbeat;

use Flow\ETL\Foundation\Progress;

final class HeartbeatStage implements Progress
{
    private int $reportedMemoryBytes = 0;

    private int $reportedPeakMemoryBytes = 0;

    private int $reportedRowsRead = 0;

    private ?string $reportedStage = null;

    private int $reportedStageRows = 0;

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

    public function reported(int $rowsRead, string $stage, int $stageRows, int $memoryBytes, int $peakMemoryBytes): void
    {
        if (
            $rowsRead === $this->reportedRowsRead
            && $stage === $this->reportedStage
            && $stageRows === $this->reportedStageRows
            && $memoryBytes === $this->reportedMemoryBytes
            && $peakMemoryBytes === $this->reportedPeakMemoryBytes
        ) {
            $this->throttle->beatWhenDue();

            return;
        }

        $addedRows = $rowsRead - $this->reportedRowsRead;
        $addedMemoryBytes = $memoryBytes - $this->reportedMemoryBytes;
        $this->reportedRowsRead = $rowsRead;
        $this->reportedStage = $stage;
        $this->reportedStageRows = $stageRows;
        $this->reportedMemoryBytes = $memoryBytes;
        $this->reportedPeakMemoryBytes = $peakMemoryBytes;

        $this->throttle->othersReported($addedRows, $stage, $stageRows, $addedMemoryBytes, $peakMemoryBytes);
    }
}
