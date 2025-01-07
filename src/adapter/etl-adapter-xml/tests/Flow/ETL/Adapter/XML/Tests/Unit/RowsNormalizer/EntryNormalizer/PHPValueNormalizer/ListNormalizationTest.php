<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML\Tests\Unit\RowsNormalizer\EntryNormalizer\PHPValueNormalizer;

use function Flow\ETL\DSL\{type_int, type_integer, type_list, type_map, type_string, type_structure};
use Flow\ETL\Adapter\XML\Abstraction\XMLNode;
use Flow\ETL\Adapter\XML\RowsNormalizer\EntryNormalizer\PHPValueNormalizer;
use Flow\ETL\PHP\Type\Caster;
use Flow\ETL\Tests\FlowTestCase;

final class ListNormalizationTest extends FlowTestCase
{
    public function test_normalization_of_list_of_flat_structures() : void
    {
        $normalizer = new PHPValueNormalizer(Caster::default());

        self::assertEquals(
            XMLNode::nestedNode('list')
                ->append(
                    XMLNode::nestedNode('element')
                        ->append(XMLNode::flatNode('name', 'John'))
                        ->append(XMLNode::flatNode('age', '30'))
                )
                ->append(
                    XMLNode::nestedNode('element')
                        ->append(XMLNode::flatNode('name', 'Jane'))
                        ->append(XMLNode::flatNode('age', '25'))
                ),
            $normalizer->normalize(
                'list',
                type_list(
                    type_structure([
                        'name' => type_string(),
                        'age' => type_int(),
                    ])
                ),
                [['name' => 'John', 'age' => 30], ['name' => 'Jane', 'age' => 25]]
            )
        );
    }

    public function test_normalizing_empty_list() : void
    {
        $normalizer = new PHPValueNormalizer(Caster::default());

        self::assertEquals(
            XMLNode::nestedNode('list'),
            $normalizer->normalize('list', type_list(type_integer()), [])
        );
    }

    public function test_normalizing_list_of_integers() : void
    {
        $normalizer = new PHPValueNormalizer(Caster::default());

        self::assertEquals(
            XMLNode::nestedNode('list')
                ->append(XMLNode::flatNode('element', '1'))
                ->append(XMLNode::flatNode('element', '2'))
                ->append(XMLNode::flatNode('element', '3')),
            $normalizer->normalize('list', type_list(type_integer()), [1, 2, 3])
        );
    }

    public function test_normalizing_list_of_list_of_integers() : void
    {
        $normalizer = new PHPValueNormalizer(Caster::default());

        self::assertEquals(
            XMLNode::nestedNode('list')
                ->append(
                    XMLNode::nestedNode('element')
                        ->append(XMLNode::flatNode('element', '1'))
                        ->append(XMLNode::flatNode('element', '2'))
                        ->append(XMLNode::flatNode('element', '3'))
                )
                ->append(
                    XMLNode::nestedNode('element')
                        ->append(XMLNode::flatNode('element', '4'))
                        ->append(XMLNode::flatNode('element', '5'))
                        ->append(XMLNode::flatNode('element', '6'))
                ),
            $normalizer->normalize('list', type_list(type_list(type_integer())), [[1, 2, 3], [4, 5, 6]])
        );
    }

    public function test_normalizing_list_of_map_of_str_to_int() : void
    {
        $normalizer = new PHPValueNormalizer(Caster::default());

        self::assertEquals(
            XMLNode::nestedNode('list')
                ->append(
                    XMLNode::nestedNode('element')
                        ->append(
                            XMLNode::nestedNode('element')
                            ->append(XMLNode::flatNode('key', 'one'))
                            ->append(XMLNode::flatNode('value', '1'))
                        )
                        ->append(
                            XMLNode::nestedNode('element')
                            ->append(XMLNode::flatNode('key', 'two'))
                            ->append(XMLNode::flatNode('value', '2'))
                        )
                )
                ->append(
                    XMLNode::nestedNode('element')
                        ->append(
                            XMLNode::nestedNode('element')
                            ->append(XMLNode::flatNode('key', 'three'))
                            ->append(XMLNode::flatNode('value', '3'))
                        )
                        ->append(
                            XMLNode::nestedNode('element')
                            ->append(XMLNode::flatNode('key', 'four'))
                            ->append(XMLNode::flatNode('value', '4'))
                        )
                ),
            $normalizer->normalize('list', type_list(type_map(type_integer(), type_string())), [['one' => 1, 'two' => 2], ['three' => 3, 'four' => 4]])
        );
    }
}
