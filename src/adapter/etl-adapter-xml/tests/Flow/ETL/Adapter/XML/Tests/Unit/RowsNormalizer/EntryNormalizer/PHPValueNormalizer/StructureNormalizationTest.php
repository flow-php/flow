<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML\Tests\Unit\RowsNormalizer\EntryNormalizer\PHPValueNormalizer;

use function Flow\ETL\DSL\{structure_element, type_datetime, type_integer, type_list, type_string, type_structure};
use Flow\ETL\Adapter\XML\Abstraction\{XMLAttribute, XMLNode};
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
                structure_element('_id', type_string()),
                structure_element('name', type_string()),
                structure_element('age', type_string()),
            ]),
            ['_id' => 1, 'name' => 'John', 'age' => 30]
        );

        self::assertEquals(
            XMLNode::nestedNode('structure')
                ->append(new XMLAttribute('id', '1'))
                ->append(XMLNode::flatNode('name', 'John'))
                ->append(XMLNode::flatNode('age', '30')),
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
                ->append(XMLNode::flatNode('name', 'John'))
                ->append(XMLNode::flatNode('age', '30'))
                ->append(
                    XMLNode::nestedNode('numbers')
                        ->append(XMLNode::flatNode('element', '1'))
                        ->append(XMLNode::flatNode('element', '2'))
                        ->append(XMLNode::flatNode('element', '3'))
                        ->append(XMLNode::flatNode('element', '4'))
                        ->append(XMLNode::flatNode('element', '5'))
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
                structure_element('_created-at', type_datetime()),
                structure_element('name', type_string()),
                structure_element('age', type_string()),
                structure_element('address', type_structure([
                    structure_element('street', type_string()),
                    structure_element('city', type_string()),
                    structure_element('zip', type_string()),
                ])),
            ]),
            ['_created-at' => new \DateTimeImmutable('2024-08-22 00:00:00'), 'name' => 'John', 'age' => 30, 'address' => ['street' => 'Main St.', 'city' => 'New York', 'zip' => '10001']]
        );

        self::assertEquals(
            XMLNode::nestedNode('structure')
                ->append(new XMLAttribute('created-at', '2024-08-22T00:00:00+00:00'))
                ->append(XMLNode::flatNode('name', 'John'))
                ->append(XMLNode::flatNode('age', '30'))
                ->append(
                    XMLNode::nestedNode('address')
                        ->append(XMLNode::flatNode('street', 'Main St.'))
                        ->append(XMLNode::flatNode('city', 'New York'))
                        ->append(XMLNode::flatNode('zip', '10001'))
                ),
            $normalized
        );
    }
}
