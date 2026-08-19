<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit\Encoding;

use Dom\HTMLDocument;
use Flow\Floe\Encoding\HtmlDocumentEncoder;
use Flow\Floe\ValueDecoder;
use PHPUnit\Framework\TestCase;

use function class_exists;
use function pack;
use function str_contains;
use function strlen;
use function substr;

final class HtmlDocumentEncoderTest extends TestCase
{
    public function test_encodes_length_prefixed_html(): void
    {
        if (!class_exists('\Dom\HTMLDocument')) {
            static::markTestSkipped('\Dom\HTMLDocument requires PHP 8.4+');
        }

        $document = ValueDecoder::htmlDocumentFromString('<p>x</p>');
        static::assertInstanceOf(HTMLDocument::class, $document);

        $encoded = (new HtmlDocumentEncoder())->encode($document);

        static::assertSame(pack('V', strlen($encoded) - 4), substr($encoded, 0, 4));
        static::assertTrue(str_contains($encoded, '<p>x</p>'));
    }
}
