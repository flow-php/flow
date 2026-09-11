<?php

declare(strict_types=1);

namespace Flow\Floe\Encoding;

use Flow\Floe\ValueEncoder as ValueEncoderFactory;

/**
 * @implements ValueEncoder<\DOMDocument>
 */
final class XmlDocumentEncoder implements ValueEncoder
{
    public function encode(mixed $value): string
    {
        return ValueEncoderFactory::lengthPrefixed(ValueEncoderFactory::xmlDocumentToString($value));
    }
}
