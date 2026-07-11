<?php

declare(strict_types=1);

namespace Flow\Floe\Encoding;

use Flow\Floe\ValueEncoder as ValueEncoderFactory;

final class HtmlDocumentEncoder implements ValueEncoder
{
    public function encode(mixed $value): string
    {
        /** @var \Dom\HTMLDocument $value */
        // @mago-ignore analysis:unavailable-method
        return ValueEncoderFactory::lengthPrefixed($value->saveHtml());
    }
}
