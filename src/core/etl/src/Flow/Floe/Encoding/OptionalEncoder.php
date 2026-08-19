<?php

declare(strict_types=1);

namespace Flow\Floe\Encoding;

use Flow\Floe\Format;

/**
 * @implements ValueEncoder<mixed>
 */
final class OptionalEncoder implements ValueEncoder
{
    public function __construct(
        private readonly ValueEncoder $base,
    ) {}

    public function encode(mixed $value): string
    {
        return $value === null ? Format::VALUE_NULL_BYTE : Format::VALUE_PRESENT_BYTE . $this->base->encode($value);
    }
}
