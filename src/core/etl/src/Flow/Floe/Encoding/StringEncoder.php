<?php

declare(strict_types=1);

namespace Flow\Floe\Encoding;

use function pack;
use function strlen;

/**
 * @implements ValueEncoder<string>
 */
final class StringEncoder implements ValueEncoder
{
    public function encode(mixed $value): string
    {
        return pack('V', strlen($value)) . $value;
    }
}
