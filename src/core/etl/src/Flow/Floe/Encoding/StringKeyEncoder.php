<?php

declare(strict_types=1);

namespace Flow\Floe\Encoding;

use function pack;
use function strlen;

/**
 * String map keys - unlike StringEncoder it casts first, because PHP turns
 * numeric-string array keys into integers.
 *
 * @implements ValueEncoder<int|string>
 */
final class StringKeyEncoder implements ValueEncoder
{
    public function encode(mixed $value): string
    {
        return pack('V', strlen((string) $value)) . $value;
    }
}
