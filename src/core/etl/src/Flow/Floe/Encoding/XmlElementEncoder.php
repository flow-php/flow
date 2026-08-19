<?php

declare(strict_types=1);

namespace Flow\Floe\Encoding;

use Flow\Floe\ValueEncoder as ValueEncoderFactory;

/**
 * @implements ValueEncoder<\DOMElement>
 */
final class XmlElementEncoder implements ValueEncoder
{
    public function encode(mixed $value): string
    {
        return ValueEncoderFactory::lengthPrefixed(ValueEncoderFactory::xmlElementToString($value));
    }
}
