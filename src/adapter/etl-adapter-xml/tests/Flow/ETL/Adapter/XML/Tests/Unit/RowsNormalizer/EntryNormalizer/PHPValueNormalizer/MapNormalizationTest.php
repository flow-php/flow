<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML\Tests\Unit\RowsNormalizer\EntryNormalizer\PHPValueNormalizer;

use function Flow\ETL\DSL\{type_integer, type_list, type_map, type_string};
use Flow\ETL\Adapter\Elasticsearch\Tests\Integration\TestCase;
use Flow\ETL\Adapter\XML\Abstraction\XMLNode;
use Flow\ETL\Adapter\XML\RowsNormalizer\EntryNormalizer\PHPValueNormalizer;
use Flow\ETL\PHP\Type\Caster;

final class MapNormalizationTest extends TestCase
{
    public function test_normalizing_empty_map_of_int_to_str() : void
    {
        $normalizer = new PHPValueNormalizer(Caster::default());

        self::assertEquals(
            XMLNode::nestedNode('map'),
            $normalizer->normalize('map', type_map(type_integer(), type_string()), [])
        );
    }

    public function test_normalizing_map_of_int_to_str() : void
    {
        $normalizer = new PHPValueNormalizer(Caster::default());

        self::assertEquals(
            XMLNode::nestedNode('map')
                ->append(
                    XMLNode::nestedNode('element')
                    ->append(XMLNode::flatNode('key', '1'))
                    ->append(XMLNode::flatNode('value', 'one'))
                )
                ->append(
                    XMLNode::nestedNode('element')
                    ->append(XMLNode::flatNode('key', '2'))
                    ->append(XMLNode::flatNode('value', 'two'))
                )
                ->append(
                    XMLNode::nestedNode('element')
                    ->append(XMLNode::flatNode('key', '3'))
                    ->append(XMLNode::flatNode('value', 'three'))
                ),
            $normalizer->normalize('map', type_map(type_integer(), type_string()), [1 => 'one', 2 => 'two', 3 => 'three'])
        );
    }

    public function test_normalizing_map_of_str_to_list_of_ints() : void
    {
        $normalizer = new PHPValueNormalizer(Caster::default());

        self::assertEquals(
            XMLNode::nestedNode('map')
                ->append(
                    XMLNode::nestedNode('element')
                    ->append(XMLNode::flatNode('key', 'one'))
                    ->append(
                        XMLNode::nestedNode('value')
                        ->append(XMLNode::flatNode('element', '1'))
                        ->append(XMLNode::flatNode('element', '2'))
                        ->append(XMLNode::flatNode('element', '3'))
                    )
                )
                ->append(
                    XMLNode::nestedNode('element')
                    ->append(XMLNode::flatNode('key', 'two'))
                    ->append(
                        XMLNode::nestedNode('value')
                        ->append(XMLNode::flatNode('element', '4'))
                        ->append(XMLNode::flatNode('element', '5'))
                        ->append(XMLNode::flatNode('element', '6'))
                    )
                ),
            $normalizer->normalize('map', type_map(type_string(), type_list(type_integer())), ['one' => [1, 2, 3], 'two' => [4, 5, 6]])
        );
    }
}
