<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit\Decoding;

use DOMDocument;
use Flow\Floe\Decoding\XmlDocumentDecoder;
use Flow\Floe\Encoding\XmlDocumentEncoder;
use Flow\Floe\Exception\FloeException;
use PHPUnit\Framework\TestCase;

use function pack;

final class XmlDocumentDecoderTest extends TestCase
{
    public function test_round_trip_restores_document(): void
    {
        $document = new DOMDocument();
        $document->loadXML('<item id="5">value</item>');

        $position = 0;
        $decoded = (new XmlDocumentDecoder())->decode((new XmlDocumentEncoder())->encode($document), $position);

        static::assertSame('item', $decoded->documentElement?->tagName);
    }

    public function test_invalid_xml_throws(): void
    {
        $position = 0;

        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('failed to restore DOMDocument');

        (new XmlDocumentDecoder())->decode(pack('V', 9) . 'not < xml', $position);
    }
}
