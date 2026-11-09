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
        if ($rows === 0) {
            $this->throttle->beatWhenDue();

            return;
        }

        $this->rows += $rows;
        $this->throttle->stageAdvanced($this->name, $this->rows);
    }

    public function ended(int $rowsRead, string $stage, int $stageRows, int $memoryBytes, int $peakMemoryBytes): void
    {
        $this->fold($rowsRead, $stage, $stageRows, $memoryBytes, $peakMemoryBytes);
    }

    public function reported(int $rowsRead, string $stage, int $stageRows, int $memoryBytes, int $peakMemoryBytes): void
    {
        $this->fold($rowsRead, $stage, $stageRows, $memoryBytes, $peakMemoryBytes);
        $this->throttle->beatWhenDue();
    }

    private function fold(int $rowsRead, string $stage, int $stageRows, int $memoryBytes, int $peakMemoryBytes): void
    {
        if (
            $rowsRead === $this->reportedRowsRead
            && $stage === $this->reportedStage
            && $stageRows === $this->reportedStageRows
            && $memoryBytes === $this->reportedMemoryBytes
            && $peakMemoryBytes === $this->reportedPeakMemoryBytes
        ) {
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
