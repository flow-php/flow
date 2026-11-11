<?php

declare(strict_types=1);

namespace Flow\ETL\Foundation;

final readonly class StageProgress
{
    /**
     * @param null|float $startedAtSeconds null: planned, not started
     */
    public function __construct(
        public ?int $id,
        public StageName $name,
        public ProgressUnit $unit,
        public ?string $subject,
        public int $count,
        public ?int $total,
        public ?float $startedAtSeconds,
        public ?float $endedAtSeconds,
    ) {}
}
