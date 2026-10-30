<?php

declare(strict_types=1);

namespace Flow\ETL\Kernels\Hash;

use Flow\ETL\Foundation\Describe\Describable;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use SensitiveParameter;

final class PlainText implements Algorithm, Describable
{
    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self();
    }

    public function describe(): ?Description
    {
        return new Description(self::class, []);
    }

    public function hash(string $value): string
    {
        return $value;
    }
}
