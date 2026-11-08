<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\Console\Tests\Mother;

use Flow\ETL\Runtime\Heartbeat\Pulse;

final class PulseMother
{
    public static function pulse(
        int $beat = 1,
        string $stage = 'sort',
        int $stageRows = 1_250_000,
        int $rowsRead = 2_000_000,
        int $memoryBytes = 148_897_792,
        int $peakMemoryBytes = 325_058_560,
        int $elapsedSeconds = 83,
    ): Pulse {
        return new Pulse(
            beat: $beat,
            elapsedSeconds: $elapsedSeconds,
            rowsRead: $rowsRead,
            stage: $stage,
            stageRows: $stageRows,
            memoryBytes: $memoryBytes,
            peakMemoryBytes: $peakMemoryBytes,
        );
    }
}
