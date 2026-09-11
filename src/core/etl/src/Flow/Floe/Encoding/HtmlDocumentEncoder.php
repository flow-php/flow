<?php

declare(strict_types=1);

namespace Flow\Floe\Encoding;

use Flow\Floe\ValueEncoder as ValueEncoderFactory;

/**
 * @implements ValueEncoder<\Dom\HTMLDocument>
 */
final class HtmlDocumentEncoder implements ValueEncoder
{
    public function encode(mixed $value): string
    {
        // @mago-ignore analysis:unavailable-method
        return ValueEncoderFactory::lengthPrefixed($value->saveHtml());
    }
}
