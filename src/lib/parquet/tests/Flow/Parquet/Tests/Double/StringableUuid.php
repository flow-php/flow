<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Double;

use Stringable;

/**
 * A uuid value object exposing its text only through __toString().
 */
final readonly class StringableUuid implements Stringable
{
    public function __construct(
        private string $text,
    ) {}

    public function __toString(): string
    {
        return $this->text;
    }
}
