<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Column\Php;

use DOMDocument;
use Flow\ETL\Column\Php\XmlDocumentPhysical;
use Flow\ETL\Exception\InvalidArgumentException;
use PHPUnit\Framework\TestCase;

use function Flow\Types\DSL\type_string;

final class XmlDocumentPhysicalTest extends TestCase
{
    public function test_the_markup(): void
    {
        $document = new DOMDocument();
        $document->loadXML('<root><a>1</a></root>');
        $physical = new XmlDocumentPhysical();
        $markup = type_string()->assert($physical->toPhysical($document));

        static::assertIsString($markup);
        static::assertStringContainsString('<root><a>1</a></root>', $markup);
        static::assertEquals($document, $physical->fromPhysical($markup));
        static::assertEquals([$document, null], $physical->fromPhysicalAll([$markup, null]));
    }

    public function test_malformed_markup_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Floe failed to restore DOMDocument from "<root>"');

        (new XmlDocumentPhysical())->fromPhysical('<root>');
    }
}
