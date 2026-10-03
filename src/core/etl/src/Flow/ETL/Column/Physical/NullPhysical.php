<?php

declare(strict_types=1);

namespace Flow\ETL\Column\Physical;

use function array_fill;
use function count;

final readonly class NullPhysical implements Physical
{
    public function toPhysical(mixed $value): mixed
    {
        return null;
    }

    public function fromPhysical(mixed $physical): mixed
    {
        return null;
    }

    public function fromPhysicalAll(array $physicals): array
    {
        return $physicals === [] ? [] : array_fill(0, count($physicals), null);
    }
}
