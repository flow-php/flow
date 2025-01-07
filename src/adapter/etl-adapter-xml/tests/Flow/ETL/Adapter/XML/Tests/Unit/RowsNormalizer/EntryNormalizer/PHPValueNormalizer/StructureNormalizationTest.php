<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML\Tests\Unit\RowsNormalizer\EntryNormalizer\PHPValueNormalizer;

use function Flow\ETL\DSL\{type_datetime, type_integer, type_list, type_string, type_structure};
use Flow\ETL\Adapter\XML\Abstraction\{XMLAttribute, XMLNode};
use Flow\ETL\Adapter\XML\RowsNormalizer\EntryNormalizer\PHPValueNormalizer;
use Flow\ETL\PHP\Type\Caster;
use Flow\ETL\Tests\FlowTestCase;

final class StructureNormalizationTest extends FlowTestCase
{
    public function test_normalization_of_flat_structure() : void
    {
        $normalizer = new PHPValueNormalizer(Caster::default());

        $normalized = $normalizer->normalize(
            'structure',
            type_structure([
                '_id' => type_string(),
                'name' => type_string(),
                'age' => type_string(),
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
                'name' => type_string(),
                'age' => type_string(),
                'numbers' => type_list(type_integer()),
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
                '_created-at' => type_datetime(),
                'name' => type_string(),
                'age' => type_string(),
                'address' => type_structure([
                    'street' => type_string(),
                    'city' => type_string(),
                    'zip' => type_string(),
                ]),
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
