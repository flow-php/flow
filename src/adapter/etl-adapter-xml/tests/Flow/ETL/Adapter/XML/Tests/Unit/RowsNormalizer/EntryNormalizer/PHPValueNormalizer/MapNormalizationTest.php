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
                ->appendChild(
                    XMLNode::nestedNode('element')
                    ->appendChild(XMLNode::flatNode('key', '1'))
                    ->appendChild(XMLNode::flatNode('value', 'one'))
                )
                ->appendChild(
                    XMLNode::nestedNode('element')
                    ->appendChild(XMLNode::flatNode('key', '2'))
                    ->appendChild(XMLNode::flatNode('value', 'two'))
                )
                ->appendChild(
                    XMLNode::nestedNode('element')
                    ->appendChild(XMLNode::flatNode('key', '3'))
                    ->appendChild(XMLNode::flatNode('value', 'three'))
                ),
            $normalizer->normalize('map', type_map(type_integer(), type_string()), [1 => 'one', 2 => 'two', 3 => 'three'])
        );
    }

    public function test_normalizing_map_of_str_to_list_of_ints() : void
    {
        $normalizer = new PHPValueNormalizer(Caster::default());

        self::assertEquals(
            XMLNode::nestedNode('map')
                ->appendChild(
                    XMLNode::nestedNode('element')
                    ->appendChild(XMLNode::flatNode('key', 'one'))
                    ->appendChild(
                        XMLNode::nestedNode('value')
                        ->appendChild(XMLNode::flatNode('element', '1'))
                        ->appendChild(XMLNode::flatNode('element', '2'))
                        ->appendChild(XMLNode::flatNode('element', '3'))
                    )
                )
                ->appendChild(
                    XMLNode::nestedNode('element')
                    ->appendChild(XMLNode::flatNode('key', 'two'))
                    ->appendChild(
                        XMLNode::nestedNode('value')
                        ->appendChild(XMLNode::flatNode('element', '4'))
                        ->appendChild(XMLNode::flatNode('element', '5'))
                        ->appendChild(XMLNode::flatNode('element', '6'))
                    )
                ),
            $normalizer->normalize('map', type_map(type_string(), type_list(type_integer())), ['one' => [1, 2, 3], 'two' => [4, 5, 6]])
        );
    }
}
