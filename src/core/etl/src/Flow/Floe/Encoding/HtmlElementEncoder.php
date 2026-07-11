<?php

declare(strict_types=1);

namespace Flow\Floe\Encoding;

use Flow\Floe\ValueEncoder as ValueEncoderFactory;

final class HtmlElementEncoder implements ValueEncoder
{
    public function encode(mixed $value): string
    {
        /** @var \Dom\HTMLElement $value */
        return ValueEncoderFactory::lengthPrefixed(ValueEncoderFactory::htmlElementToString($value));
    }
}
