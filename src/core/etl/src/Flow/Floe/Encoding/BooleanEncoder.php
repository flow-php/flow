<?php

declare(strict_types=1);

namespace Flow\Floe\Encoding;

/**
 * @implements ValueEncoder<bool>
 */
final class BooleanEncoder implements ValueEncoder
{
    public function encode(mixed $value): string
    {
        return $value ? "\x01" : "\x00";
    }
}
