<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\Console\Tests\Mother;

use Flow\ETL\Foundation\ProgressUnit;
use Flow\ETL\Foundation\StageProgress;
use Flow\ETL\Runtime\Heartbeat\Pulse;

final class PulseMother
{
    /**
     * @param null|list<StageProgress> $stages replaces the one open entry that $stage, $count and $unit build
     */
    public static function pulse(
        int $beat = 1,
        string $stage = 'sort',
        int $count = 1_250_000,
        int $rowsRead = 2_000_000,
        int $memoryBytes = 148_897_792,
        int $peakMemoryBytes = 325_058_560,
        float $elapsedSeconds = 83.0,
        float $idleSeconds = 0.0,
        ProgressUnit $unit = ProgressUnit::rows,
        ?array $stages = null,
    ): Pulse {
        return new Pulse(
            beat: $beat,
            elapsedSeconds: $elapsedSeconds,
            rowsRead: $rowsRead,
            stages: $stages ?? [StageProgressMother::open($stage, $count, unit: $unit)],
            memoryBytes: $memoryBytes,
            peakMemoryBytes: $peakMemoryBytes,
            idleSeconds: $idleSeconds,
        );
    }
}
