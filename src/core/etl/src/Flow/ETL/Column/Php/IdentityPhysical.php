<?php

declare(strict_types=1);

namespace Flow\ETL\Column\Php;

final readonly class IdentityPhysical implements Physical
{
    public function toPhysical(mixed $value): mixed
    {
        return $value;
    }

    public function fromPhysical(mixed $physical): mixed
    {
        return $physical;
    }

    public function fromPhysicalAll(array $physicals): array
    {
        return $physicals;
    }
}
