<?php

declare(strict_types=1);

namespace Flow\Floe\Encoding;

use Flow\Floe\ValueEncoder as ValueEncoderFactory;

final class XmlElementEncoder implements ValueEncoder
{
    public function encode(mixed $value): string
    {
        /** @var \DOMElement $value */
        return ValueEncoderFactory::lengthPrefixed(ValueEncoderFactory::xmlElementToString($value));
    }
}
