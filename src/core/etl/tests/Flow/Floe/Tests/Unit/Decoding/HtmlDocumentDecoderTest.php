<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit\Decoding;

use Flow\Floe\Decoding\HtmlDocumentDecoder;
use Flow\Floe\Encoding\HtmlDocumentEncoder;
use Flow\Floe\ValueDecoder;
use PHPUnit\Framework\TestCase;

use function class_exists;
use function strlen;

final class HtmlDocumentDecoderTest extends TestCase
{
    public function test_round_trip_restores_document(): void
    {
        if (!class_exists('\Dom\HTMLDocument')) {
            static::markTestSkipped('\Dom\HTMLDocument requires PHP 8.4+');
        }

        $encoder = new HtmlDocumentEncoder();
        $encoded = $encoder->encode(ValueDecoder::htmlDocumentFromString('<p>x</p>'));
        $position = 0;

        $decoded = (new HtmlDocumentDecoder())->decode($encoded, $position);

        static::assertSame($encoded, $encoder->encode($decoded));
        static::assertSame(strlen($encoded), $position);
    }
}
