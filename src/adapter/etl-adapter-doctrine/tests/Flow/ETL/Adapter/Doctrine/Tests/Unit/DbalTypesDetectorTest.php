<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\Tests\Unit;

use function Flow\ETL\DSL\{bool_schema, datetime_schema, float_schema, integer_schema, json_schema, list_schema, map_schema, schema, string_schema, structure_schema};
use function Flow\Types\DSL\{type_integer, type_list, type_map, type_string, type_structure};
use Doctrine\DBAL\Types\{Type, Types};
use Flow\ETL\Adapter\Doctrine\DbalTypesDetector;
use PHPUnit\Framework\TestCase;

final class DbalTypesDetectorTest extends TestCase
{
    public function test_converts_empty_schema() : void
    {
        $converter = new DbalTypesDetector();

        $schema = schema();

        $types = $converter->convert($schema);

        self::assertCount(0, $types);
    }

    public function test_converts_flow_schema_to_dbal_types() : void
    {
        $converter = new DbalTypesDetector();

        $schema = schema(string_schema('name'), integer_schema('age'), float_schema('score'), bool_schema('active'), datetime_schema('created_at'));

        $types = $converter->convert($schema);

        self::assertCount(5, $types);
        self::assertInstanceOf(Type::class, $types['name']);
        self::assertSame(Types::STRING, Type::getTypeRegistry()->lookupName($types['name']));
        self::assertSame(Types::INTEGER, Type::getTypeRegistry()->lookupName($types['age']));
        self::assertSame(Types::FLOAT, Type::getTypeRegistry()->lookupName($types['score']));
        self::assertSame(Types::BOOLEAN, Type::getTypeRegistry()->lookupName($types['active']));
        self::assertSame(Types::DATETIME_IMMUTABLE, Type::getTypeRegistry()->lookupName($types['created_at']));
    }

    public function test_converts_json_type_to_json() : void
    {
        $converter = new DbalTypesDetector();

        $schema = schema(json_schema('data'));

        $types = $converter->convert($schema);

        self::assertCount(1, $types);
        self::assertSame(Types::JSON, Type::getTypeRegistry()->lookupName($types['data']));
    }

    public function test_converts_nested_types_to_json() : void
    {
        $converter = new DbalTypesDetector();

        $schema = schema(list_schema('items', type_list(type_string())), map_schema('metadata', type_map(type_string(), type_string())), structure_schema('config', type_structure(['field1' => type_string(), 'field2' => type_integer()])));

        $types = $converter->convert($schema);

        self::assertCount(3, $types);
        self::assertSame(Types::JSON, Type::getTypeRegistry()->lookupName($types['items']));
        self::assertSame(Types::JSON, Type::getTypeRegistry()->lookupName($types['metadata']));
        self::assertSame(Types::JSON, Type::getTypeRegistry()->lookupName($types['config']));
    }
}
