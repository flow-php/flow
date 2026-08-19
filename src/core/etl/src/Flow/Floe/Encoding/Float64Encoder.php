<?php

declare(strict_types=1);

namespace Flow\Floe\Encoding;

use function pack;

/**
 * @implements ValueEncoder<float>
 */
final class Float64Encoder implements ValueEncoder
{
    public function encode(mixed $value): string
    {
        return pack('e', $value);
    }
}
