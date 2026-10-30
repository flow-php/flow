<?php

declare(strict_types=1);

namespace Flow\ETL\Kernels\Sort;

use Flow\ETL\Foundation\Describe\Describable;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use SensitiveParameter;

final readonly class MemorySortConfig implements Describable
{
    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self();
    }

    public function describe(): ?Description
    {
        return new Description(self::class, []);
    }
}
