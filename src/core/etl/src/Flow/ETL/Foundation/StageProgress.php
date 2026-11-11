<?php

declare(strict_types=1);

namespace Flow\ETL\Foundation;

final readonly class StageProgress
{
    public function __construct(
        public ?int $id,
        public string $name,
        public ProgressUnit $unit,
        public ?string $location,
        public int $count,
        public ?int $total,
        public float $startedAtSeconds,
        public ?float $endedAtSeconds,
    ) {}
}
