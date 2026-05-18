<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML\Tests\Unit\RowsNormalizer\EntryNormalizer;

use DateTimeImmutable;
use Flow\ETL\Adapter\XML\Abstraction\XMLAttribute;
use Flow\ETL\Adapter\XML\Abstraction\XMLNode;
use Flow\ETL\Adapter\XML\RowsNormalizer\EntryNormalizer\PHPValueNormalizer;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Types\Value\Json;
use stdClass;

use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_instance_of;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_json;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;

final class PHPValueNormalizerTest extends FlowTestCase
{
    public function test_normalizing_array_type(): void
    {
        $normalizer = new PHPValueNormalizer();

        static::assertEquals(
            XMLNode::flatNode('array', '{"a":"1","b":22}'),
            $normalizer->normalize('array', type_array(), ['a' => '1', 'b' => 22]),
        );
    }

    public function test_normalizing_attribute(): void
    {
        $normalizer = new PHPValueNormalizer();

        static::assertEquals(
            new XMLAttribute('attribute', 'a'),
            $normalizer->normalize('_attribute', type_string(), 'a'),
        );
    }

    public function test_normalizing_boolean_type(): void
    {
        $normalizer = new PHPValueNormalizer();

        static::assertEquals(XMLNode::flatNode('bool', 'false'), $normalizer->normalize('bool', type_boolean(), false));
        static::assertEquals(XMLNode::flatNode('bool', 'true'), $normalizer->normalize('bool', type_boolean(), true));
    }

    public function test_normalizing_datetime_type(): void
    {
        $normalizer = new PHPValueNormalizer();

        static::assertEquals(
            XMLNode::flatNode('array', '2024-08-22T02:00:00.000000+00:00'),
            $normalizer->normalize('array', type_datetime(), new DateTimeImmutable('2024-08-22 02:00:00 UTC')),
        );
    }

    public function test_normalizing_float_type(): void
    {
        $normalizer = new PHPValueNormalizer();

        static::assertEquals(XMLNode::flatNode('float', '1.1'), $normalizer->normalize('float', type_float(), 1.1));
    }

    public function test_normalizing_integer_type(): void
    {
        $normalizer = new PHPValueNormalizer();

        static::assertEquals(XMLNode::flatNode('int', '1'), $normalizer->normalize('int', type_integer(), 1));

        static::assertEquals(
            XMLNode::flatNode('int', ''),
            $normalizer->normalize('int', type_optional(type_integer()), null),
        );
    }

    public function test_normalizing_json_type(): void
    {
        $normalizer = new PHPValueNormalizer();

        static::assertEquals(
            XMLNode::flatNode('json', '{"a":"1","b":22}'),
            $normalizer->normalize('json', type_json(), Json::fromArray(['a' => '1', 'b' => 22])),
        );
    }

    public function test_normalizing_object_type(): void
    {
        static::markTestSkipped('We need to figure out what to do with object types');

        /** @phpstan-ignore-next-line */
        $normalizer = new PHPValueNormalizer();

        static::assertEquals(
            XMLNode::flatNode('object', '{"a":"1","b":22}'),
            $normalizer->normalize('object', type_instance_of(stdClass::class), (object) ['a' => '1', 'b' => 22]),
        );
    }

    public function test_normalizing_string_type(): void
    {
        $normalizer = new PHPValueNormalizer();

        static::assertEquals(XMLNode::flatNode('str', 'a'), $normalizer->normalize('str', type_string(), 'a'));
    }
}
