<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit\Encoding;

use Dom\HTMLElement;
use Flow\Floe\Encoding\HtmlElementEncoder;
use Flow\Floe\ValueDecoder;
use Flow\Floe\ValueEncoder;
use PHPUnit\Framework\TestCase;

use function class_exists;
use function pack;
use function strlen;

final class HtmlElementEncoderTest extends TestCase
{
    public function test_encodes_length_prefixed_element_html(): void
    {
        if (!class_exists('\Dom\HTMLDocument')) {
            static::markTestSkipped('\Dom\HTMLDocument requires PHP 8.4+');
        }

        $element = ValueDecoder::htmlElementFromString('<p>x</p>');
        static::assertInstanceOf(HTMLElement::class, $element);
        $html = ValueEncoder::htmlElementToString($element);

        static::assertSame(pack('V', strlen($html)) . $html, (new HtmlElementEncoder())->encode($element));
    }
}
