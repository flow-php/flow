<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML\Tests\Unit\Abstraction;

use Flow\ETL\Adapter\XML\Abstraction\XMLAttribute;
use Flow\ETL\Adapter\XML\Abstraction\XMLNode;
use Flow\ETL\Adapter\XML\Abstraction\XMLNodeType;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;

final class XMLNodeTest extends FlowTestCase
{
    public function test_nested_splits_its_elements_into_attributes_and_children_in_order(): void
    {
        $first = XMLNode::flatNode('first', '1');
        $second = XMLNode::flatNode('second', '2');
        $id = new XMLAttribute('id', '7');

        $node = XMLNode::nested('row', $first, $id, $second);

        static::assertSame(XMLNodeType::NESTED, $node->type);
        static::assertNull($node->value);
        static::assertSame([$id], $node->attributes);
        static::assertSame([$first, $second], $node->children);
    }

    public function test_nested_equals_the_same_node_built_by_appending(): void
    {
        $child = XMLNode::flatNode('child', 'value');
        $attribute = new XMLAttribute('id', '1');

        static::assertEquals(
            XMLNode::nestedNode('row')->append($child)->append($attribute),
            XMLNode::nested('row', $child, $attribute),
        );
    }

    public function test_an_empty_name_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('XMLNode name can not be empty');

        XMLNode::nested('');
    }
}
