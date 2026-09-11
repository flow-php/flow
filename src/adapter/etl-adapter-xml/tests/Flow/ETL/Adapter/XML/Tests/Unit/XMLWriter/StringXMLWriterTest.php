<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML\Tests\Unit\XMLWriter;

use DOMException;
use Flow\ETL\Adapter\XML\Abstraction\XMLAttribute;
use Flow\ETL\Adapter\XML\Abstraction\XMLNode;
use Flow\ETL\Adapter\XML\XMLWriter\DOMDocumentWriter;
use Flow\ETL\Adapter\XML\XMLWriter\StringXMLWriter;
use Flow\ETL\Tests\FlowTestCase;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\TestWith;

final class StringXMLWriterTest extends FlowTestCase
{
    /**
     * @return Generator<string, array{XMLNode}>
     */
    public static function nodes(): Generator
    {
        yield 'a flat node' => [XMLNode::flatNode('id', '1')];
        yield 'an empty string value' => [XMLNode::flatNode('id', '')];
        yield 'a null value' => [XMLNode::flatNode('id', null)];
        yield 'an empty nested node' => [XMLNode::nestedNode('root')];
        yield 'nested children and attributes' => [XMLNode::nested(
            'row',
            new XMLAttribute('id', '1'),
            XMLNode::flatNode('name', 'a'),
            XMLNode::nested('items', XMLNode::flatNode('element', '1'), XMLNode::nestedNode('element')),
        )];
        yield 'an attribute set twice keeps its first position and its last value' => [XMLNode::nested(
            'row',
            new XMLAttribute('a', '1'),
            new XMLAttribute('b', '2'),
            new XMLAttribute('a', '3'),
        )];
        yield 'a prefixed name' => [XMLNode::flatNode('a:b', '1')];
        yield 'a non-ascii name' => [XMLNode::flatNode('ünï', '1')];

        yield 'text with non-ascii text' => [XMLNode::flatNode('v', 'ünïcødé ☃')];
        yield 'text with invalid UTF-8' => [XMLNode::flatNode('v', "a\xC3\x28b")];
        yield 'text with a noncharacter' => [XMLNode::flatNode('v', "a\u{FFFE}b")];

        foreach ([
            'markup characters' => "a&b<c>d\"e'f",
            'line breaks and tabs' => "line\r\nbreak\ttab",
            'a CDATA terminator' => ']]>',
            'an entity look-alike' => '&amp;',
            'a DEL character' => "a\x7Fb",
            'a NUL in the middle' => "a\x00b",
            'a leading NUL' => "\x00a",
            'only whitespace' => '  ',
        ] as $label => $value) {
            yield 'text with ' . $label => [XMLNode::flatNode('v', $value)];
            yield 'an attribute with ' . $label => [XMLNode::nested('row', new XMLAttribute('a', $value))];
        }
    }

    /**
     * DOMDocument writes these as below on libxml 2.14 and later only - older libxml escapes them differently.
     *
     * @return Generator<string, array{XMLNode, string}>
     */
    public static function nodesOlderLibxmlWritesDifferently(): Generator
    {
        yield 'text with control characters' => [XMLNode::flatNode('v', "a\x01b\x1Fc"), '<v>a&#xFFFD;b&#xFFFD;c</v>'];
        yield 'an attribute with control characters' => [
            XMLNode::nested('row', new XMLAttribute('a', "a\x01b\x1Fc")),
            '<row a="a&#xFFFD;b&#xFFFD;c"/>',
        ];
        yield 'an attribute with non-ascii text' => [
            XMLNode::nested('row', new XMLAttribute('a', 'ünïcødé ☃')),
            '<row a="ünïcødé ☃"/>',
        ];
        yield 'an attribute with invalid UTF-8' => [
            XMLNode::nested('row', new XMLAttribute('a', "a\xC3\x28b")),
            "<row a=\"a\xC3\x28b\"/>",
        ];
        yield 'an attribute with a noncharacter' => [
            XMLNode::nested('row', new XMLAttribute('a', "a\u{FFFE}b")),
            "<row a=\"a\u{FFFE}b\"/>",
        ];
    }

    #[TestWith(['order id'])]
    #[TestWith(['1abc'])]
    #[TestWith(['-x'])]
    public function test_an_attribute_name_dom_refuses_is_refused_with_its_exception(string $name): void
    {
        $this->expectException(DOMException::class);
        $this->expectExceptionMessage('Invalid Character Error');

        (new StringXMLWriter())->write(XMLNode::nested('row', new XMLAttribute($name, '1')));
    }

    #[TestWith(['order id'])]
    #[TestWith(['1abc'])]
    #[TestWith(['-x'])]
    public function test_an_element_name_dom_refuses_is_refused_with_its_exception(string $name): void
    {
        $this->expectException(DOMException::class);
        $this->expectExceptionMessage('Invalid Character Error');

        (new StringXMLWriter())->write(XMLNode::flatNode($name, '1'));
    }

    public function test_a_refused_name_stays_refused(): void
    {
        $writer = new StringXMLWriter();

        try {
            $writer->write(XMLNode::flatNode('order id', '1'));
        } catch (DOMException) {
        }

        $this->expectException(DOMException::class);

        $writer->write(XMLNode::flatNode('order id', '1'));
    }

    #[DataProvider('nodes')]
    public function test_writes_what_dom_document_writer_writes(XMLNode $node): void
    {
        static::assertSame((new DOMDocumentWriter())->write($node), (new StringXMLWriter())->write($node));
    }

    #[DataProvider('nodesOlderLibxmlWritesDifferently')]
    public function test_writes_what_dom_document_writer_writes_on_libxml_2_14_and_later(
        XMLNode $node,
        string $xml,
    ): void {
        static::assertSame($xml, (new StringXMLWriter())->write($node));
    }
}
