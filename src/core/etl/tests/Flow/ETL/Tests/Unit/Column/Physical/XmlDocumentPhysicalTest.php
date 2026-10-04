<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Column\Physical;

use DOMDocument;
use DOMElement;
use Flow\ETL\Column\Physical\XmlDocumentPhysical;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\Mother\XmlNodeMother;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\TestWith;
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

    public static function nodes(): Generator
    {
        foreach (XmlNodeMother::edges() as $label => ['node' => $node]) {
            yield $label => [$node];
        }
    }

    #[DataProvider('nodes')]
    public function test_text_is_the_inverse_of_physical(string $node): void
    {
        $physical = new XmlDocumentPhysical();

        static::assertSame($node, $physical->text($physical->physical($node)));
    }

    #[TestWith(["<?xml version=\"1.0\"?>\n<a/>\n"])]
    #[TestWith(["<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<!-- c -->\n<a/>\n"])]
    #[TestWith(["<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<a/>\n<!-- c -->\n"])]
    #[TestWith(["<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<?pi x?>\n<a/>\n"])]
    #[TestWith(["<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<a/>\n<?pi x?>\n"])]
    #[TestWith(["<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<!DOCTYPE a>\n<a/>\n"])]
    #[TestWith(["<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<?xml version=\"1.0\"?><a/>\n"])]
    #[TestWith(["<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<a/>"])]
    #[TestWith(["<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\n"])]
    #[TestWith(['<a/>'])]
    public function test_bytes_physical_did_not_produce_have_no_text(string $physical): void
    {
        static::assertNull((new XmlDocumentPhysical())->text($physical));
    }

    public function test_the_physical_of_a_node_is_the_markup_of_its_utf8_document(): void
    {
        $document = new DOMDocument('1.0', 'UTF-8');
        $row = new DOMElement('row');
        $document->appendChild($row);
        $row->setAttribute('a', 'żółć');
        $row->appendChild(new DOMElement('b', 'ść'));

        static::assertSame(
            (new XmlDocumentPhysical())->toPhysical($document),
            (new XmlDocumentPhysical())->physical('<row a="żółć"><b>ść</b></row>'),
        );
    }
}
