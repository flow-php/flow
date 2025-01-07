<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML\Tests\Unit\RowsNormalizer;

use function Flow\ETL\DSL\{str_entry,
    structure_entry,
    type_boolean,
    type_datetime,
    type_integer,
    type_list,
    type_map,
    type_string,
    type_structure};
use Flow\ETL\Adapter\XML\Abstraction\{XMLAttribute, XMLNode};
use Flow\ETL\Adapter\XML\RowsNormalizer\EntryNormalizer;
use Flow\ETL\Adapter\XML\RowsNormalizer\EntryNormalizer\PHPValueNormalizer;
use Flow\ETL\PHP\Type\Caster;
use Flow\ETL\Tests\FlowTestCase;

final class EntryNormalizerTest extends FlowTestCase
{
    public function test_normalization_entries_into_attributes() : void
    {
        $entryNormalizer = new EntryNormalizer(new PHPValueNormalizer(Caster::default()));

        self::assertEquals(
            new XMLAttribute('id', '1'),
            $entryNormalizer->normalize(str_entry('_id', '1'))
        );
    }

    public function test_normalizing_structure_entry() : void
    {
        $entryNormalizer = new EntryNormalizer(new PHPValueNormalizer(Caster::default()));

        $structure = structure_entry(
            'structure',
            [
                'id' => 1,
                'name' => 'name',
                'active' => true,
                'date' => new \DateTimeImmutable('2024-04-04 00:00:00 UTC'),
                'list' => [1, 2, 3],
                'map' => ['a' => 1, 'b' => 2],
                'nested_structure' => [
                    'id' => 2,
                    'name' => 'nested-name',
                    'active' => false,
                    'date' => new \DateTimeImmutable('2024-04-04 00:00:00 UTC'),
                    'list' => [4, 5, 6],
                    'map' => ['c' => 3, 'd' => 4],
                ],
            ],
            type_structure([
                'id' => type_integer(),
                'name' => type_string(),
                'active' => type_boolean(),
                'date' => type_datetime(),
                'list' => type_list(type_integer()),
                'map' => type_map(type_string(), type_integer()),
                'nested_structure' => type_structure([
                    'id' => type_integer(),
                    'name' => type_string(),
                    'active' => type_boolean(),
                    'date' => type_datetime(),
                    'list' => type_list(type_integer()),
                    'map' => type_map(type_string(), type_integer()),
                ]),
            ])
        );

        self::assertEquals(
            XMLNode::nestedNode('structure')
                ->append(XMLNode::flatNode('id', '1'))
                ->append(XMLNode::flatNode('name', 'name'))
                ->append(XMLNode::flatNode('active', 'true'))
                ->append(XMLNode::flatNode('date', '2024-04-04T00:00:00.000000+00:00'))
                ->append(
                    XMLNode::nestedNode('list')
                    ->append(XMLNode::flatNode('element', '1'))
                    ->append(XMLNode::flatNode('element', '2'))
                    ->append(XMLNode::flatNode('element', '3'))
                )
                ->append(
                    XMLNode::nestedNode('map')
                    ->append(
                        XMLNode::nestedNode('element')
                        ->append(XMLNode::flatNode('key', 'a'))
                        ->append(XMLNode::flatNode('value', '1'))
                    )
                    ->append(
                        XMLNode::nestedNode('element')
                        ->append(XMLNode::flatNode('key', 'b'))
                        ->append(XMLNode::flatNode('value', '2'))
                    )
                )
                ->append(
                    XMLNode::nestedNode('nested_structure')
                    ->append(XMLNode::flatNode('id', '2'))
                    ->append(XMLNode::flatNode('name', 'nested-name'))
                    ->append(XMLNode::flatNode('active', 'false'))
                    ->append(XMLNode::flatNode('date', '2024-04-04T00:00:00.000000+00:00'))
                    ->append(
                        XMLNode::nestedNode('list')
                        ->append(XMLNode::flatNode('element', '4'))
                        ->append(XMLNode::flatNode('element', '5'))
                        ->append(XMLNode::flatNode('element', '6'))
                    )
                    ->append(
                        XMLNode::nestedNode('map')
                        ->append(
                            XMLNode::nestedNode('element')
                            ->append(XMLNode::flatNode('key', 'c'))
                            ->append(XMLNode::flatNode('value', '3'))
                        )
                        ->append(
                            XMLNode::nestedNode('element')
                            ->append(XMLNode::flatNode('key', 'd'))
                            ->append(XMLNode::flatNode('value', '4'))
                        )
                    )
                ),
            $entryNormalizer->normalize($structure)
        );
    }
}
