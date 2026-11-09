<?php

declare(strict_types=1);

namespace Flow\ETL\Runtime\Heartbeat;

final readonly class Pulse
{
    public function __construct(
        public int $beat,
        public int $elapsedSeconds,
        public int $rowsRead,
        public string $stage,
        public int $stageRows,
        public int $memoryBytes,
        public int $peakMemoryBytes,
        public int $idleSeconds,
    ) {}
}
