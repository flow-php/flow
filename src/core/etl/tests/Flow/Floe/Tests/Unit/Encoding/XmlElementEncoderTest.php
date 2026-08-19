<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit\Encoding;

use DOMDocument;
use DOMElement;
use Flow\Floe\Encoding\XmlElementEncoder;
use PHPUnit\Framework\TestCase;

use function pack;
use function strlen;

final class XmlElementEncoderTest extends TestCase
{
    public function test_encodes_length_prefixed_element_xml(): void
    {
        $document = new DOMDocument();
        $document->loadXML('<item id="5">value</item>');
        $element = $document->documentElement;
        static::assertInstanceOf(DOMElement::class, $element);

        static::assertSame(
            pack('V', strlen('<item id="5">value</item>')) . '<item id="5">value</item>',
            (new XmlElementEncoder())->encode($element),
        );
    }
}
