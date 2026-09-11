<?php

declare(strict_types=1);

namespace Flow\Floe\Encoding;

use Flow\Floe\ValueEncoder as ValueEncoderFactory;

/**
 * @implements ValueEncoder<\Dom\HTMLElement>
 */
final class HtmlElementEncoder implements ValueEncoder
{
    public function encode(mixed $value): string
    {
        return ValueEncoderFactory::lengthPrefixed(ValueEncoderFactory::htmlElementToString($value));
    }
}
