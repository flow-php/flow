<?php

declare(strict_types=1);

namespace Flow\Floe\Encoding;

final class BooleanEncoder implements ValueEncoder
{
    public function encode(mixed $value): string
    {
        return $value ? "\x01" : "\x00";
    }
}
