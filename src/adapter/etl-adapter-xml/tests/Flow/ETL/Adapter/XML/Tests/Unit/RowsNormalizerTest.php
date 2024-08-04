<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML\Tests\Unit;

use function Flow\ETL\DSL\{row,
    rows,
    str_entry,
    structure_element,
    structure_entry,
    type_boolean,
    type_datetime,
    type_integer,
    type_list,
    type_map,
    type_string,
    type_structure};
use Flow\ETL\Adapter\XML\Abstraction\{XMLAttribute, XMLNode};
use Flow\ETL\Adapter\XML\RowsNormalizer;
use Flow\ETL\Adapter\XML\RowsNormalizer\EntryNormalizer;
use Flow\ETL\Adapter\XML\RowsNormalizer\EntryNormalizer\PHPValueNormalizer;
use Flow\ETL\PHP\Type\Caster;
use PHPUnit\Framework\TestCase;

final class RowsNormalizerTest extends TestCase
{
    public function test_normalization_of_rows() : void
    {
        $normalizer = new RowsNormalizer(new EntryNormalizer(new PHPValueNormalizer(Caster::default())));

        $rows = rows(
            row(
                structure_entry(
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
                        structure_element('id', type_integer()),
                        structure_element('name', type_string()),
                        structure_element('active', type_boolean()),
                        structure_element('date', type_datetime()),
                        structure_element('list', type_list(type_integer())),
                        structure_element('map', type_map(type_string(), type_integer())),
                        structure_element('nested_structure', type_structure([
                            structure_element('id', type_integer()),
                            structure_element('name', type_string()),
                            structure_element('active', type_boolean()),
                            structure_element('date', type_datetime()),
                            structure_element('list', type_list(type_integer())),
                            structure_element('map', type_map(type_string(), type_integer())),
                        ])),
                    ])
                )
            )
        );

        self::assertEquals(
            XMLNode::nestedNode('row')
                ->append(
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
                        )
                ),
            \iterator_to_array($normalizer->normalize($rows))[0]
        );
    }

    public function test_normalizing_rows_with_attributes() : void
    {
        $normalizer = new RowsNormalizer(new EntryNormalizer(new PHPValueNormalizer(Caster::default())));

        self::assertEquals(
            XMLNode::nestedNode('row')
                ->append(new XMLAttribute('id', '1'))
                ->append(XMLNode::flatNode('name', 'John Doe')),
            \iterator_to_array($normalizer->normalize(
                rows(
                    row(
                        str_entry('_id', '1'),
                        str_entry('name', 'John Doe')
                    )
                )
            ))[0]
        );
    }
}
