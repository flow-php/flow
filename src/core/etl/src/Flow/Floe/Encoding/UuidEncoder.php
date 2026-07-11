<?php

declare(strict_types=1);

namespace Flow\Floe\Encoding;

final class UuidEncoder implements ValueEncoder
{
    public function encode(mixed $value): string
    {
        /** @var \Flow\Types\Value\Uuid $value */
        return $value->toString();
    }
}
