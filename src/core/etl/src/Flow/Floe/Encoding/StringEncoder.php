<?php

declare(strict_types=1);

namespace Flow\Floe\Encoding;

use function pack;
use function strlen;

final class StringEncoder implements ValueEncoder
{
    public function encode(mixed $value): string
    {
        /** @var string $value */
        return pack('V', strlen($value)) . $value;
    }
}
