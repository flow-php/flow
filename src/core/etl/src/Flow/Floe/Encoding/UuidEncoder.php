<?php

declare(strict_types=1);

namespace Flow\Floe\Encoding;

use Flow\Floe\ValueEncoder as ValueEncoderFactory;

/**
 * @implements ValueEncoder<\Flow\Types\Value\Uuid>
 */
final class UuidEncoder implements ValueEncoder
{
    public function encode(mixed $value): string
    {
        return ValueEncoderFactory::lengthPrefixed($value->toString());
    }
}
