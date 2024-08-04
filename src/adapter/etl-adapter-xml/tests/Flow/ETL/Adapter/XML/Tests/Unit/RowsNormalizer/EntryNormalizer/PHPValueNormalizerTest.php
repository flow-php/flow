<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML\Tests\Unit\RowsNormalizer\EntryNormalizer;

use function Flow\ETL\DSL\{type_array, type_boolean, type_datetime, type_float, type_integer, type_json, type_object, type_string};
use Flow\ETL\Adapter\XML\Abstraction\XMLNode;
use Flow\ETL\Adapter\XML\RowsNormalizer\EntryNormalizer\PHPValueNormalizer;
use Flow\ETL\PHP\Type\Caster;
use PHPUnit\Framework\TestCase;

final class PHPValueNormalizerTest extends TestCase
{
    public function test_normalizing_array_type() : void
    {
        $normalizer = new PHPValueNormalizer(Caster::default());

        self::assertEquals(
            XMLNode::flatNode('array', '{"a":"1","b":22}'),
            $normalizer->normalize('array', type_array(), ['a' => '1', 'b' => 22])
        );
    }

    public function test_normalizing_boolean_type() : void
    {
        $normalizer = new PHPValueNormalizer(Caster::default());

        self::assertEquals(
            XMLNode::flatNode('bool', 'false'),
            $normalizer->normalize('bool', type_boolean(), false)
        );
        self::assertEquals(
            XMLNode::flatNode('bool', 'true'),
            $normalizer->normalize('bool', type_boolean(), true)
        );
    }

    public function test_normalizing_datetime_type() : void
    {
        $normalizer = new PHPValueNormalizer(Caster::default());

        self::assertEquals(
            XMLNode::flatNode('array', '2024-08-22T02:00:00.000000+00:00'),
            $normalizer->normalize('array', type_datetime(), new \DateTimeImmutable('2024-08-22 02:00:00 UTC'))
        );
    }

    public function test_normalizing_float_type() : void
    {
        $normalizer = new PHPValueNormalizer(Caster::default());

        self::assertEquals(
            XMLNode::flatNode('float', '1.1'),
            $normalizer->normalize('float', type_float(), 1.1)
        );
    }

    public function test_normalizing_integer_type() : void
    {
        $normalizer = new PHPValueNormalizer(Caster::default());

        self::assertEquals(
            XMLNode::flatNode('int', '1'),
            $normalizer->normalize('int', type_integer(), 1)
        );

        self::assertEquals(
            XMLNode::flatNode('int', ''),
            $normalizer->normalize('int', type_integer(true), null)
        );
    }

    public function test_normalizing_json_type() : void
    {
        $normalizer = new PHPValueNormalizer(Caster::default());

        self::assertEquals(
            XMLNode::flatNode('json', '{"a":"1","b":22}'),
            $normalizer->normalize('json', type_json(), ['a' => '1', 'b' => 22])
        );
    }

    public function test_normalizing_object_type() : void
    {
        self::markTestSkipped('We need to figure out what to do with object types');

        $normalizer = new PHPValueNormalizer(Caster::default());

        self::assertEquals(
            XMLNode::flatNode('object', '{"a":"1","b":22}'),
            $normalizer->normalize('object', type_object(\stdClass::class), (object) ['a' => '1', 'b' => 22])
        );
    }

    public function test_normalizing_string_type() : void
    {
        $normalizer = new PHPValueNormalizer(Caster::default());

        self::assertEquals(
            XMLNode::flatNode('str', 'a'),
            $normalizer->normalize('str', type_string(), 'a')
        );
    }
}
