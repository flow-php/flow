<?php

declare(strict_types=1);

namespace Flow\Floe\Encoding;

use function pack;

final class Int64Encoder implements ValueEncoder
{
    public function encode(mixed $value): string
    {
        return pack('P', $value);
    }
}
