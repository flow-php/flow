<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\Console\Tests\Mother;

use Flow\ETL\Foundation\ProgressUnit;
use Flow\ETL\Foundation\StageProgress;

final class StageProgressMother
{
    public static function done(
        string $name = 'sort',
        int $count = 30_000_000,
        float $startedAtSeconds = 0.0,
        float $endedAtSeconds = 6.0,
        ?int $id = 1,
        ?string $location = null,
        ProgressUnit $unit = ProgressUnit::rows,
        ?int $total = null,
    ): StageProgress {
        return new StageProgress($id, $name, $unit, $location, $count, $total, $startedAtSeconds, $endedAtSeconds);
    }

    public static function open(
        string $name = 'sort',
        int $count = 15_000_000,
        ?int $total = null,
        float $startedAtSeconds = 0.0,
        ?int $id = 1,
        ?string $location = null,
        ProgressUnit $unit = ProgressUnit::rows,
    ): StageProgress {
        return new StageProgress($id, $name, $unit, $location, $count, $total, $startedAtSeconds, null);
    }
}
