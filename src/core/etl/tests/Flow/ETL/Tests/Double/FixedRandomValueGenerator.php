<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\RandomValueGenerator;

final readonly class FixedRandomValueGenerator implements RandomValueGenerator
{
    public function __construct(
        private string $string,
        private int $int = 0,
    ) {}

    public function int(int $min, int $max): int
    {
        return $this->int;
    }

    public function string(int $int): string
    {
        return $this->string;
    }
}
