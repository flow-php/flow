<?php

declare(strict_types=1);

namespace Flow\ETL\Foundation;

interface ProgressStages
{
    public function stage(
        StageName $stage,
        ProgressUnit $unit = ProgressUnit::rows,
        ?int $id = null,
        ?string $subject = null,
        ?int $total = null,
    ): Progress;
}
