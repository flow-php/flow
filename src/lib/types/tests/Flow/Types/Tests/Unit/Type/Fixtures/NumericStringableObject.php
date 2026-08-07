<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type\Fixtures;

use Stringable;

final class NumericStringableObject implements Stringable
{
    public function __construct(
        private readonly string $value = '1234.5',
    ) {}

    public function __toString(): string
    {
        return $this->value;
    }
}
