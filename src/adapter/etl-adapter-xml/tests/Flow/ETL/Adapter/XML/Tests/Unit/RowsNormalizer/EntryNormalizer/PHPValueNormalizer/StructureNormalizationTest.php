<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML\Tests\Unit\RowsNormalizer\EntryNormalizer\PHPValueNormalizer;

use function Flow\ETL\DSL\{structure_element, type_integer, type_list, type_string, type_structure};
use Flow\ETL\Adapter\XML\Abstraction\XMLNode;
use Flow\ETL\Adapter\XML\RowsNormalizer\EntryNormalizer\PHPValueNormalizer;
use Flow\ETL\PHP\Type\Caster;
use PHPUnit\Framework\TestCase;

final class StructureNormalizationTest extends TestCase
{
    public function test_normalization_of_flat_structure() : void
    {
        $normalizer = new PHPValueNormalizer(Caster::default());

        $normalized = $normalizer->normalize(
            'structure',
            type_structure([
                structure_element('name', type_string()),
                structure_element('age', type_string()),
            ]),
            ['name' => 'John', 'age' => 30]
        );

        self::assertEquals(
            XMLNode::nestedNode('structure')
                ->appendChild(XMLNode::flatNode('name', 'John'))
                ->appendChild(XMLNode::flatNode('age', '30')),
            $normalized
        );
    }

    public function test_normalization_of_structure_with_list_of_int() : void
    {
        $normalizer = new PHPValueNormalizer(Caster::default());

        $normalized = $normalizer->normalize(
            'structure',
            type_structure([
                structure_element('name', type_string()),
                structure_element('age', type_string()),
                structure_element('numbers', type_list(type_integer())),
            ]),
            ['name' => 'John', 'age' => 30, 'numbers' => [1, 2, 3, 4, 5]]
        );

        self::assertEquals(
            XMLNode::nestedNode('structure')
                ->appendChild(XMLNode::flatNode('name', 'John'))
                ->appendChild(XMLNode::flatNode('age', '30'))
                ->appendChild(
                    XMLNode::nestedNode('numbers')
                        ->appendChild(XMLNode::flatNode('element', '1'))
                        ->appendChild(XMLNode::flatNode('element', '2'))
                        ->appendChild(XMLNode::flatNode('element', '3'))
                        ->appendChild(XMLNode::flatNode('element', '4'))
                        ->appendChild(XMLNode::flatNode('element', '5'))
                ),
            $normalized
        );
    }

    public function test_normalization_of_structure_with_nested_structure() : void
    {
        $normalizer = new PHPValueNormalizer(Caster::default());

        $normalized = $normalizer->normalize(
            'structure',
            type_structure([
                structure_element('name', type_string()),
                structure_element('age', type_string()),
                structure_element('address', type_structure([
                    structure_element('street', type_string()),
                    structure_element('city', type_string()),
                    structure_element('zip', type_string()),
                ])),
            ]),
            ['name' => 'John', 'age' => 30, 'address' => ['street' => 'Main St.', 'city' => 'New York', 'zip' => '10001']]
        );

        self::assertEquals(
            XMLNode::nestedNode('structure')
                ->appendChild(XMLNode::flatNode('name', 'John'))
                ->appendChild(XMLNode::flatNode('age', '30'))
                ->appendChild(
                    XMLNode::nestedNode('address')
                        ->appendChild(XMLNode::flatNode('street', 'Main St.'))
                        ->appendChild(XMLNode::flatNode('city', 'New York'))
                        ->appendChild(XMLNode::flatNode('zip', '10001'))
                ),
            $normalized
        );
    }
}
