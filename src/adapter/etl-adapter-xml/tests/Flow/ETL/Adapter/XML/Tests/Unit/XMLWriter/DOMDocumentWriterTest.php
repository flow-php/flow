<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML\Tests\Unit\XMLWriter;

use DOMException;
use Flow\ETL\Adapter\XML\Abstraction\XMLAttribute;
use Flow\ETL\Adapter\XML\Abstraction\XMLNode;
use Flow\ETL\Adapter\XML\Tests\Context\XMLWriterValues;
use Flow\ETL\Adapter\XML\XMLWriter\DOMDocumentWriter;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Types\Exception\CastingException;
use PHPUnit\Framework\Attributes\DataProviderExternal;

use function substr;

final class DOMDocumentWriterTest extends FlowTestCase
{
    public function test_writing_empty_child_node(): void
    {
        $xmlWriter = new DOMDocumentWriter();

        static::assertEquals(
            '<root><child/></root>',
            $xmlWriter->write(XMLNode::nestedNode('root')->append(XMLNode::nestedNode('child'))),
        );
    }

    public function test_writing_empty_node(): void
    {
        $xmlWriter = new DOMDocumentWriter();

        static::assertEquals('<root/>', $xmlWriter->write(XMLNode::nestedNode('root')));
    }

    public function test_writing_node_with_empty_string_value(): void
    {
        $xmlWriter = new DOMDocumentWriter();

        static::assertEquals('<root></root>', $xmlWriter->write(XMLNode::flatNode('root', '')));
    }

    public function test_writing_xml(): void
    {
        $xmlWriter = new DOMDocumentWriter();

        static::assertEquals(
            '<root><child>value</child><child_with_children><child>value</child><child>value</child></child_with_children></root>',
            $xmlWriter->write(
                XMLNode::nestedNode('root')
                    ->append(XMLNode::flatNode('child', 'value'))
                    ->append(
                        XMLNode::nestedNode('child_with_children')
                            ->append(XMLNode::flatNode('child', 'value'))
                            ->append(XMLNode::flatNode('child', 'value')),
                    ),
            ),
        );
    }

    public function test_writing_xml_with_attribute(): void
    {
        $xmlWriter = new DOMDocumentWriter();

        static::assertEquals(
            '<root attribute="value">value</root>',
            $xmlWriter->write(XMLNode::flatNode('root', 'value')->appendAttribute(new XMLAttribute(
                'attribute',
                'value',
            ))),
        );
    }

    public function test_writing_xml_with_empty_attribute(): void
    {
        $xmlWriter = new DOMDocumentWriter();

        static::assertEquals(
            '<root attribute="">value</root>',
            $xmlWriter->write(XMLNode::flatNode('root', 'value')->appendAttribute(new XMLAttribute('attribute', ''))),
        );
    }

    #[DataProviderExternal(XMLWriterValues::class, 'values')]
    public function test_elements_are_what_write_renders_for_a_flat_node(string $value): void
    {
        $writer = new DOMDocumentWriter();

        static::assertSame(
            [$writer->write(XMLNode::flatNode('v', $value)), $writer->write(XMLNode::flatNode('v', 'second'))],
            $writer->elements('v', [$value, 'second']),
        );
    }

    #[DataProviderExternal(XMLWriterValues::class, 'values')]
    public function test_attributes_are_what_write_renders_for_an_attribute(string $value): void
    {
        $writer = new DOMDocumentWriter();

        static::assertSame(
            [
                substr($writer->write(XMLNode::nested('x', new XMLAttribute('a', $value))), 2, -2),
                ' a="second"',
            ],
            $writer->attributes('a', [$value, 'second']),
        );
    }

    public function test_a_null_element_is_an_empty_element(): void
    {
        static::assertSame(
            ['<v></v>', '<v>x</v>', '<v></v>'],
            (new DOMDocumentWriter())->elements('v', [null, 'x', '']),
        );
    }

    public function test_a_null_attribute_is_refused(): void
    {
        $this->expectException(CastingException::class);

        (new DOMDocumentWriter())->attributes('a', ['x', null]);
    }

    public function test_no_values_give_no_elements_and_no_attributes(): void
    {
        static::assertSame([], (new DOMDocumentWriter())->elements('v', []));
        static::assertSame([], (new DOMDocumentWriter())->attributes('a', []));
    }

    public function test_elements_of_a_name_dom_refuses_are_refused_with_its_exception(): void
    {
        $this->expectException(DOMException::class);

        (new DOMDocumentWriter())->elements('1 bad', ['x']);
    }

    public function test_attributes_of_a_name_dom_refuses_are_refused_with_its_exception(): void
    {
        $this->expectException(DOMException::class);

        (new DOMDocumentWriter())->attributes('1 bad', ['x']);
    }
}
