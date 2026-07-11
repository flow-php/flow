<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit\Decoding;

use DOMDocument;
use Flow\Floe\Decoding\XmlElementDecoder;
use Flow\Floe\Encoding\XmlElementEncoder;
use PHPUnit\Framework\TestCase;

final class XmlElementDecoderTest extends TestCase
{
    public function test_round_trip_restores_element(): void
    {
        $document = new DOMDocument();
        $document->loadXML('<item id="5">value</item>');

        $position = 0;
        $decoded = (new XmlElementDecoder())->decode(
            (new XmlElementEncoder())->encode($document->documentElement),
            $position,
        );

        static::assertSame('item', $decoded->tagName);
        static::assertSame('5', $decoded->getAttribute('id'));
    }
}
