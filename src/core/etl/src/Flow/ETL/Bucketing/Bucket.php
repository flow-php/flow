<?php

declare(strict_types=1);

namespace Flow\ETL\Bucketing;

final readonly class Bucket
{
    public function __construct(
        public string $id,
        public int $totalRows,
        public int $index,
    ) {}
}
