<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit\Decoding;

use Dom\HTMLDocument;
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

        $document = ValueDecoder::htmlDocumentFromString('<p>x</p>');
        static::assertInstanceOf(HTMLDocument::class, $document);

        $encoder = new HtmlDocumentEncoder();
        $encoded = $encoder->encode($document);
        $position = 0;

        $decoded = (new HtmlDocumentDecoder())->decode($encoded, $position);
        static::assertInstanceOf(HTMLDocument::class, $decoded);

        static::assertSame($encoded, $encoder->encode($decoded));
        static::assertSame(strlen($encoded), $position);
    }
}
