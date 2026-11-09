<?php

declare(strict_types=1);

namespace Flow\ETL\Foundation;

final readonly class VoidProgress implements Progress, ProgressStages
{
    public function advanced(int $rows): void {}

    public function ended(int $rowsRead, string $stage, int $stageRows, int $memoryBytes, int $peakMemoryBytes): void {}

    public function reported(
        int $rowsRead,
        string $stage,
        int $stageRows,
        int $memoryBytes,
        int $peakMemoryBytes,
    ): void {}

    public function stage(string $name): Progress
    {
        return $this;
    }
}
