<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Double;

/**
 * A uuid value object exposing its text through toString(), as Flow\Types\Value\Uuid does.
 */
final readonly class ToStringUuid
{
    public function __construct(
        private string $text,
    ) {}

    public function toString(): string
    {
        return $this->text;
    }
}
