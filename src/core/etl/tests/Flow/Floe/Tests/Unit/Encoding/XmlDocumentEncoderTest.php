<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit\Encoding;

use DOMDocument;
use Flow\Floe\Encoding\XmlDocumentEncoder;
use Flow\Floe\ValueEncoder;
use PHPUnit\Framework\TestCase;

use function pack;
use function strlen;

final class XmlDocumentEncoderTest extends TestCase
{
    public function test_encodes_length_prefixed_xml(): void
    {
        $document = new DOMDocument();
        $document->loadXML('<item id="5">value</item>');

        $xml = ValueEncoder::xmlDocumentToString($document);

        static::assertSame(pack('V', strlen($xml)) . $xml, (new XmlDocumentEncoder())->encode($document));
    }
}
