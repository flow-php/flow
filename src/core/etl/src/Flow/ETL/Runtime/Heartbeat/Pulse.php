<?php

declare(strict_types=1);

namespace Flow\ETL\Runtime\Heartbeat;

use Flow\ETL\Foundation\StageProgress;

final readonly class Pulse
{
    /**
     * @param list<StageProgress> $stages
     */
    public function __construct(
        public int $beat,
        public float $elapsedSeconds,
        public int $rowsRead,
        public array $stages,
        public int $memoryBytes,
        public int $peakMemoryBytes,
        public float $idleSeconds,
    ) {}
}
