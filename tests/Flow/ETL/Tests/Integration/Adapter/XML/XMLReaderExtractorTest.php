<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Adapter\XML;

use Flow\ETL\Adapter\XML\XMLReaderExtractor;
use PHPUnit\Framework\TestCase;

final class XMLReaderExtractorTest extends TestCase
{
    public function test_reading_xml() : void
    {
        $extractor = new XMLReaderExtractor(__DIR__ . '/xml/simple_items.xml', 'root/items/item', 5, 'row');
        $rowsGenerator = $extractor->extract();

        $this->assertSame(5, $rowsGenerator->current()->count());
        $this->assertInstanceOf(\DOMDocument::class, $xml = $rowsGenerator->current()->first()->valueOf('row'));
        $this->assertXmlStringEqualsXmlString('<item><id>1</id></item>', $xml->saveHTML());

        $rowsGenerator->next();

        $this->assertSame(1, $rowsGenerator->current()->count());
        $this->assertInstanceOf(\DOMDocument::class, $xml = $rowsGenerator->current()->first()->valueOf('row'));
        $this->assertXmlStringEqualsXmlString('<item><id>6</id></item>', $xml->saveHTML());
    }
}
