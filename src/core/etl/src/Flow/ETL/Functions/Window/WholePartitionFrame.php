<?php

declare(strict_types=1);

namespace Flow\ETL\Functions\Window;

use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Values\Rows;
use SensitiveParameter;

final readonly class WholePartitionFrame implements WindowFrame
{
    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self();
    }

    public function describe(): ?Description
    {
        return new Description(self::class, []);
    }

    public function bounds(int $index, Rows $partition): array
    {
        return [0, $partition->count() - 1];
    }
}
