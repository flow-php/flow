<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\Console\Tests\Mother;

use Flow\ETL\Foundation\ProgressUnit;
use Flow\ETL\Foundation\StageName;
use Flow\ETL\Foundation\StageProgress;

final class StageProgressMother
{
    public static function done(
        StageName $name = StageName::sort,
        int $count = 30_000_000,
        float $startedAtSeconds = 0.0,
        float $endedAtSeconds = 6.0,
        ?int $id = 1,
        ?string $subject = null,
        ProgressUnit $unit = ProgressUnit::rows,
        ?int $total = null,
    ): StageProgress {
        return new StageProgress($id, $name, $unit, $subject, $count, $total, $startedAtSeconds, $endedAtSeconds);
    }

    public static function open(
        StageName $name = StageName::sort,
        int $count = 15_000_000,
        ?int $total = null,
        float $startedAtSeconds = 0.0,
        ?int $id = 1,
        ?string $subject = null,
        ProgressUnit $unit = ProgressUnit::rows,
    ): StageProgress {
        return new StageProgress($id, $name, $unit, $subject, $count, $total, $startedAtSeconds, null);
    }

    public static function planned(
        StageName $name = StageName::sort,
        ?int $id = 2,
        ?string $subject = null,
    ): StageProgress {
        return new StageProgress($id, $name, ProgressUnit::rows, $subject, 0, null, null, null);
    }

    public static function skipped(
        StageName $name = StageName::sort,
        float $endedAtSeconds = 6.0,
        ?int $id = 2,
        ?string $subject = null,
    ): StageProgress {
        return new StageProgress($id, $name, ProgressUnit::rows, $subject, 0, null, null, $endedAtSeconds);
    }
}
