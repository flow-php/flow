<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit\Decoding;

use Dom\HTMLElement;
use Flow\Floe\Decoding\HtmlElementDecoder;
use Flow\Floe\Encoding\HtmlElementEncoder;
use Flow\Floe\ValueDecoder;
use Flow\Floe\ValueEncoder;
use PHPUnit\Framework\TestCase;

use function class_exists;

final class HtmlElementDecoderTest extends TestCase
{
    public function test_round_trip_restores_element(): void
    {
        if (!class_exists('\Dom\HTMLDocument')) {
            static::markTestSkipped('\Dom\HTMLDocument requires PHP 8.4+');
        }

        $element = ValueDecoder::htmlElementFromString('<p>x</p>');
        static::assertInstanceOf(HTMLElement::class, $element);

        $position = 0;

        $decoded = (new HtmlElementDecoder())->decode((new HtmlElementEncoder())->encode($element), $position);
        static::assertInstanceOf(HTMLElement::class, $decoded);

        static::assertSame(ValueEncoder::htmlElementToString($element), ValueEncoder::htmlElementToString($decoded));
    }
}
