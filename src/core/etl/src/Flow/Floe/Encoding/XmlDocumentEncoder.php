<?php

declare(strict_types=1);

namespace Flow\Floe\Encoding;

use Flow\Floe\ValueEncoder as ValueEncoderFactory;

final class XmlDocumentEncoder implements ValueEncoder
{
    public function encode(mixed $value): string
    {
        /** @var \DOMDocument $value */
        return ValueEncoderFactory::lengthPrefixed(ValueEncoderFactory::xmlDocumentToString($value));
    }
}
