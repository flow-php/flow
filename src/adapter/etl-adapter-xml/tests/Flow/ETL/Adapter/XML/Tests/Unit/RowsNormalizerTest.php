<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML\Tests\Unit;

use function Flow\ETL\DSL\{row, rows, structure_element, structure_entry, type_boolean, type_datetime, type_integer, type_list, type_map, type_string, type_structure};
use Flow\ETL\Adapter\XML\Abstraction\XMLNode;
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
                ->appendChild(
                    XMLNode::nestedNode('structure')
                        ->appendChild(XMLNode::flatNode('id', '1'))
                        ->appendChild(XMLNode::flatNode('name', 'name'))
                        ->appendChild(XMLNode::flatNode('active', 'true'))
                        ->appendChild(XMLNode::flatNode('date', '2024-04-04T00:00:00.000000+00:00'))
                        ->appendChild(
                            XMLNode::nestedNode('list')
                            ->appendChild(XMLNode::flatNode('element', '1'))
                            ->appendChild(XMLNode::flatNode('element', '2'))
                            ->appendChild(XMLNode::flatNode('element', '3'))
                        )
                        ->appendChild(
                            XMLNode::nestedNode('map')
                            ->appendChild(
                                XMLNode::nestedNode('element')
                                ->appendChild(XMLNode::flatNode('key', 'a'))
                                ->appendChild(XMLNode::flatNode('value', '1'))
                            )
                            ->appendChild(
                                XMLNode::nestedNode('element')
                                ->appendChild(XMLNode::flatNode('key', 'b'))
                                ->appendChild(XMLNode::flatNode('value', '2'))
                            )
                        )
                        ->appendChild(
                            XMLNode::nestedNode('nested_structure')
                            ->appendChild(XMLNode::flatNode('id', '2'))
                            ->appendChild(XMLNode::flatNode('name', 'nested-name'))
                            ->appendChild(XMLNode::flatNode('active', 'false'))
                            ->appendChild(XMLNode::flatNode('date', '2024-04-04T00:00:00.000000+00:00'))
                            ->appendChild(
                                XMLNode::nestedNode('list')
                                ->appendChild(XMLNode::flatNode('element', '4'))
                                ->appendChild(XMLNode::flatNode('element', '5'))
                                ->appendChild(XMLNode::flatNode('element', '6'))
                            )
                            ->appendChild(
                                XMLNode::nestedNode('map')
                                ->appendChild(
                                    XMLNode::nestedNode('element')
                                    ->appendChild(XMLNode::flatNode('key', 'c'))
                                    ->appendChild(XMLNode::flatNode('value', '3'))
                                )
                                ->appendChild(
                                    XMLNode::nestedNode('element')
                                    ->appendChild(XMLNode::flatNode('key', 'd'))
                                    ->appendChild(XMLNode::flatNode('value', '4'))
                                )
                            )
                        )
                ),
            \iterator_to_array($normalizer->normalize($rows))[0]
        );
    }
}
