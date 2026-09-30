<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML\Tests\Unit;

use Flow\ETL\Adapter\XML\Abstraction\XMLAttribute;
use Flow\ETL\Adapter\XML\Abstraction\XMLNode;
use Flow\ETL\Adapter\XML\XMLValueNodes;
use Flow\ETL\Column\TextValues;
use Flow\ETL\Exception\RuntimeException;
use Flow\Types\Exception\CastingException;
use Flow\Types\Type;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

use function Flow\Types\DSL\structure_element;
use function Flow\Types\DSL\type_date;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_json;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;

final class XMLValueNodesTest extends TestCase
{
    /**
     * @return Generator<string, array{Type<mixed>, mixed, XMLNode}>
     */
    public static function nodes(): Generator
    {
        yield 'null' => [type_optional(type_list(type_integer())), null, XMLNode::flatNode('v', '')];
        yield 'a leaf' => [type_float(), 1.0, XMLNode::flatNode('v', '1.0')];
        yield 'list' => [
            type_list(type_string()),
            ['a', 'b'],
            XMLNode::nested('v', XMLNode::flatNode('element', 'a'), XMLNode::flatNode('element', 'b')),
        ];
        yield 'an empty list' => [type_list(type_string()), [], XMLNode::nestedNode('v')];
        yield 'list<?integer>' => [
            type_list(type_optional(type_integer())),
            [1, null],
            XMLNode::nested('v', XMLNode::flatNode('element', '1'), XMLNode::flatNode('element', '')),
        ];
        yield 'list<date>' => [
            type_list(type_date()),
            [20_455],
            XMLNode::nested('v', XMLNode::flatNode('element', '2026-01-02')),
        ];
        yield 'list<json> keeps the stored text' => [
            type_list(type_json()),
            ['{"a": 1}'],
            XMLNode::nested('v', XMLNode::flatNode('element', '{"a": 1}')),
        ];
        yield 'map' => [
            type_map(type_string(), type_optional(type_integer())),
            ['x' => 1, 'y' => null],
            XMLNode::nested(
                'v',
                XMLNode::nested('element', XMLNode::flatNode('key', 'x'), XMLNode::flatNode('value', '1')),
                XMLNode::nested('element', XMLNode::flatNode('key', 'y'), XMLNode::flatNode('value', '')),
            ),
        ];
        yield 'a map keyed by integers' => [
            type_map(type_integer(), type_string()),
            [5 => 'a'],
            XMLNode::nested('v', XMLNode::nested(
                'element',
                XMLNode::flatNode('key', '5'),
                XMLNode::flatNode('value', 'a'),
            )),
        ];
        yield 'an empty map' => [type_map(type_string(), type_integer()), [], XMLNode::nestedNode('v')];
        yield 'structure' => [
            type_structure(['d' => type_datetime(), 'l' => type_list(type_integer())]),
            ['d' => 1_767_323_045_000_000, 'l' => [1]],
            XMLNode::nested(
                'v',
                XMLNode::flatNode('d', '2026-01-02T03:04:05+00:00'),
                XMLNode::nested('l', XMLNode::flatNode('element', '1')),
            ),
        ];
        yield 'a structure element whose name carries the prefix is an attribute' => [
            type_structure(['_id' => type_integer(), 'city' => type_string()]),
            ['_id' => 7, 'city' => 'Krakow'],
            XMLNode::nested('v', new XMLAttribute('id', '7'), XMLNode::flatNode('city', 'Krakow')),
        ];
    }

    /**
     * @param Type<mixed> $type
     */
    #[DataProvider('nodes')]
    public function test_of(Type $type, mixed $physical, XMLNode $expected): void
    {
        static::assertEquals($expected, (new XMLValueNodes(new TextValues()))->of('v', $type, $physical));
    }

    public function test_of_uses_the_configured_names_and_prefix(): void
    {
        $nodes = new XMLValueNodes(new TextValues(), '@', 'item', 'entry', '@k', 'val');

        static::assertEquals(
            XMLNode::nested('v', XMLNode::flatNode('item', 'a')),
            $nodes->of('v', type_list(type_string()), ['a']),
        );
        static::assertEquals(
            XMLNode::nested('v', XMLNode::nested('entry', new XMLAttribute('k', 'x'), XMLNode::flatNode('val', '1'))),
            $nodes->of('v', type_map(type_string(), type_integer()), ['x' => 1]),
        );
    }

    public function test_a_member_without_the_prefix_is_a_node(): void
    {
        static::assertEquals(
            XMLNode::flatNode('id', '7'),
            (new XMLValueNodes(new TextValues()))->member('id', type_integer(), 7),
        );
    }

    public function test_a_null_member_that_is_an_attribute_is_refused(): void
    {
        $this->expectException(CastingException::class);

        (new XMLValueNodes(new TextValues()))->member('_id', type_optional(type_integer()), null);
    }

    public function test_a_structure_with_an_optional_element_is_refused_at_any_depth(): void
    {
        $nodes = new XMLValueNodes(new TextValues());
        $optional = type_structure(['zip' => structure_element('zip', type_string(), optional: true)]);
        $refused = 0;

        foreach ([
            $optional,
            type_list($optional),
            type_map(type_string(), $optional),
            type_structure(['s' => $optional]),
        ] as $type) {
            try {
                $nodes->refuseOptionalElements($type);
            } catch (RuntimeException $refusal) {
                static::assertSame(
                    'XML encoder does not support structure optional elements, given: structure{zip?: string}',
                    $refusal->getMessage(),
                );
                $refused++;
            }
        }

        static::assertSame(4, $refused);
    }

    public function test_types_without_optional_elements_are_accepted(): void
    {
        $this->expectNotToPerformAssertions();

        (new XMLValueNodes(new TextValues()))->refuseOptionalElements(type_list(type_structure([
            'a' => type_optional(type_integer()),
            'm' => type_map(type_string(), type_date()),
        ])));
    }
}
