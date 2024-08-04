<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML\Tests\Unit\XMLWriter;

use Flow\ETL\Adapter\XML\Abstraction\{XMLAttribute, XMLNode};
use Flow\ETL\Adapter\XML\XMLWriter\SimpleXMLWriter;
use PHPUnit\Framework\TestCase;

final class SimpleXMLWriterTest extends TestCase
{
    public function test_writing_empty_child_node() : void
    {
        $xmlWriter = new SimpleXMLWriter();

        self::assertEquals(
            '<root><child/></root>',
            $xmlWriter->write(
                XMLNode::nestedNode('root')
                    ->append(XMLNode::nestedNode('child'))
            )
        );
    }

    public function test_writing_empty_node() : void
    {
        $xmlWriter = new SimpleXMLWriter();

        self::assertEquals(
            '<root/>',
            $xmlWriter->write(
                XMLNode::nestedNode('root')
            )
        );
    }

    public function test_writing_xml() : void
    {
        $xmlWriter = new SimpleXMLWriter();

        self::assertEquals(
            '<root><child>value</child><child_with_children><child>value</child><child>value</child></child_with_children></root>',
            $xmlWriter->write(
                XMLNode::nestedNode('root')
                    ->append(XMLNode::flatNode('child', 'value'))
                    ->append(
                        XMLNode::nestedNode('child_with_children')
                            ->append(XMLNode::flatNode('child', 'value'))
                            ->append(XMLNode::flatNode('child', 'value'))
                    )
            )
        );
    }

    public function test_writing_xml_with_attribute() : void
    {
        $xmlWriter = new SimpleXMLWriter();

        self::assertEquals(
            '<root attribute="value">value</root>',
            $xmlWriter->write(
                XMLNode::flatNode('root', 'value')
                    ->appendAttribute(new XMLAttribute('attribute', 'value'))
            )
        );
    }

    public function test_writing_xml_with_empty_attribute() : void
    {
        $xmlWriter = new SimpleXMLWriter();

        self::assertEquals(
            '<root attribute="">value</root>',
            $xmlWriter->write(
                XMLNode::flatNode('root', 'value')
                    ->appendAttribute(new XMLAttribute('attribute', ''))
            )
        );
    }
}
