<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML\Tests\Unit\RowsNormalizer\EntryNormalizer\PHPValueNormalizer;

use function Flow\ETL\DSL\{structure_element, type_int, type_integer, type_list, type_map, type_string, type_structure};
use Flow\ETL\Adapter\XML\Abstraction\XMLNode;
use Flow\ETL\Adapter\XML\RowsNormalizer\EntryNormalizer\PHPValueNormalizer;
use Flow\ETL\PHP\Type\Caster;
use PHPUnit\Framework\TestCase;

final class ListNormalizationTest extends TestCase
{
    public function test_normalization_of_list_of_flat_structures() : void
    {
        $normalizer = new PHPValueNormalizer(Caster::default());

        self::assertEquals(
            XMLNode::nestedNode('list')
                ->appendChild(
                    XMLNode::nestedNode('element')
                        ->appendChild(XMLNode::flatNode('name', 'John'))
                        ->appendChild(XMLNode::flatNode('age', '30'))
                )
                ->appendChild(
                    XMLNode::nestedNode('element')
                        ->appendChild(XMLNode::flatNode('name', 'Jane'))
                        ->appendChild(XMLNode::flatNode('age', '25'))
                ),
            $normalizer->normalize(
                'list',
                type_list(
                    type_structure([
                        structure_element('name', type_string()),
                        structure_element('age', type_int()),
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
                ->appendChild(XMLNode::flatNode('element', '1'))
                ->appendChild(XMLNode::flatNode('element', '2'))
                ->appendChild(XMLNode::flatNode('element', '3')),
            $normalizer->normalize('list', type_list(type_integer()), [1, 2, 3])
        );
    }

    public function test_normalizing_list_of_list_of_integers() : void
    {
        $normalizer = new PHPValueNormalizer(Caster::default());

        self::assertEquals(
            XMLNode::nestedNode('list')
                ->appendChild(
                    XMLNode::nestedNode('element')
                        ->appendChild(XMLNode::flatNode('element', '1'))
                        ->appendChild(XMLNode::flatNode('element', '2'))
                        ->appendChild(XMLNode::flatNode('element', '3'))
                )
                ->appendChild(
                    XMLNode::nestedNode('element')
                        ->appendChild(XMLNode::flatNode('element', '4'))
                        ->appendChild(XMLNode::flatNode('element', '5'))
                        ->appendChild(XMLNode::flatNode('element', '6'))
                ),
            $normalizer->normalize('list', type_list(type_list(type_integer())), [[1, 2, 3], [4, 5, 6]])
        );
    }

    public function test_normalizing_list_of_map_of_str_to_int() : void
    {
        $normalizer = new PHPValueNormalizer(Caster::default());

        self::assertEquals(
            XMLNode::nestedNode('list')
                ->appendChild(
                    XMLNode::nestedNode('element')
                        ->appendChild(
                            XMLNode::nestedNode('element')
                            ->appendChild(XMLNode::flatNode('key', 'one'))
                            ->appendChild(XMLNode::flatNode('value', '1'))
                        )
                        ->appendChild(
                            XMLNode::nestedNode('element')
                            ->appendChild(XMLNode::flatNode('key', 'two'))
                            ->appendChild(XMLNode::flatNode('value', '2'))
                        )
                )
                ->appendChild(
                    XMLNode::nestedNode('element')
                        ->appendChild(
                            XMLNode::nestedNode('element')
                            ->appendChild(XMLNode::flatNode('key', 'three'))
                            ->appendChild(XMLNode::flatNode('value', '3'))
                        )
                        ->appendChild(
                            XMLNode::nestedNode('element')
                            ->appendChild(XMLNode::flatNode('key', 'four'))
                            ->appendChild(XMLNode::flatNode('value', '4'))
                        )
                ),
            $normalizer->normalize('list', type_list(type_map(type_integer(), type_string())), [['one' => 1, 'two' => 2], ['three' => 3, 'four' => 4]])
        );
    }
}
