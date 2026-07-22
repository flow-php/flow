<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql\Tests\Unit;

use DateTimeImmutable;
use Flow\ETL\Adapter\PostgreSql\EntryTypesMap;
use Flow\ETL\Adapter\PostgreSql\Exception\TypeMappingException;
use Flow\PostgreSql\Client\TypedValue;
use Flow\PostgreSql\Client\Types\ValueType;
use Flow\PostgreSql\QueryBuilder\Schema\ColumnType;
use Flow\Types\Type\Logical\JsonType;
use Flow\Types\Type\Native\IntegerType;
use Flow\Types\Type\Native\StringType;
use PHPUnit\Framework\TestCase;

use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_json;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function Flow\Types\DSL\type_uuid;
use function Flow\Types\DSL\type_xml;

final class EntryTypesMapTest extends TestCase
{
    public function test_allows_override_for_integer_type_to_int2(): void
    {
        $map = new EntryTypesMap([
            IntegerType::class => ValueType::INT2,
        ]);

        $result = $map->map('small_count', type_integer(), 42);

        static::assertInstanceOf(TypedValue::class, $result);
        static::assertSame(ValueType::INT2, $result->targetType);
        static::assertSame(42, $result->value);
    }

    public function test_maps_boolean_type_to_bool(): void
    {
        $result = (new EntryTypesMap())->map('active', type_boolean(), true);

        static::assertInstanceOf(TypedValue::class, $result);
        static::assertSame(ValueType::BOOL, $result->targetType);
        static::assertTrue($result->value);
    }

    public function test_maps_datetime_type_to_timestamp(): void
    {
        $date = new DateTimeImmutable('2024-01-15 10:30:00');
        $result = (new EntryTypesMap())->map('created_at', type_datetime(), $date);

        static::assertInstanceOf(TypedValue::class, $result);
        static::assertSame(ValueType::TIMESTAMP, $result->targetType);
        static::assertEquals($date, $result->value);
    }

    public function test_maps_float_type_to_float8(): void
    {
        $result = (new EntryTypesMap())->map('price', type_float(), 99.99);

        static::assertInstanceOf(TypedValue::class, $result);
        static::assertSame(ValueType::FLOAT8, $result->targetType);
        static::assertSame(99.99, $result->value);
    }

    public function test_maps_integer_type_to_int8(): void
    {
        $result = (new EntryTypesMap())->map('count', type_integer(), 42);

        static::assertInstanceOf(TypedValue::class, $result);
        static::assertSame(ValueType::INT8, $result->targetType);
        static::assertSame(42, $result->value);
    }

    public function test_maps_json_type_to_jsonb(): void
    {
        $result = (new EntryTypesMap())->map('data', type_json(), ['key' => 'value']);

        static::assertInstanceOf(TypedValue::class, $result);
        static::assertSame(ValueType::JSONB, $result->targetType);
    }

    public function test_maps_list_type_to_jsonb(): void
    {
        $result = (new EntryTypesMap())->map('tags', type_list(type_integer()), [1, 2, 3]);

        static::assertInstanceOf(TypedValue::class, $result);
        static::assertSame(ValueType::JSONB, $result->targetType);
        static::assertSame([1, 2, 3], $result->value);
    }

    public function test_maps_map_type_to_jsonb(): void
    {
        $result = (new EntryTypesMap())->map('metadata', type_map(type_string(), type_string()), ['key' => 'value']);

        static::assertInstanceOf(TypedValue::class, $result);
        static::assertSame(ValueType::JSONB, $result->targetType);
        static::assertSame(['key' => 'value'], $result->value);
    }

    public function test_maps_null_value_to_null(): void
    {
        static::assertNull((new EntryTypesMap())->map('name', type_string(), null));
    }

    public function test_maps_string_type_to_text(): void
    {
        $result = (new EntryTypesMap())->map('name', type_string(), 'Alice');

        static::assertInstanceOf(TypedValue::class, $result);
        static::assertSame(ValueType::TEXT, $result->targetType);
        static::assertSame('Alice', $result->value);
    }

    public function test_maps_structure_type_to_jsonb(): void
    {
        $result = (new EntryTypesMap())->map(
            'user',
            type_structure(['name' => type_string(), 'age' => type_integer()]),
            ['name' => 'Alice', 'age' => 30],
        );

        static::assertInstanceOf(TypedValue::class, $result);
        static::assertSame(ValueType::JSONB, $result->targetType);
    }

    public function test_maps_uuid_type_to_uuid(): void
    {
        $result = (new EntryTypesMap())->map('id', type_uuid(), '550e8400-e29b-41d4-a716-446655440000');

        static::assertInstanceOf(TypedValue::class, $result);
        static::assertSame(ValueType::UUID, $result->targetType);
    }

    public function test_to_column_type_allows_override(): void
    {
        $map = new EntryTypesMap([], [
            StringType::class => ColumnType::varchar(255),
        ]);

        static::assertTrue($map->toColumnType(type_string())->isEqual(ColumnType::varchar(255)));
    }

    public function test_to_column_type_maps_integer_to_bigint(): void
    {
        $map = new EntryTypesMap();

        static::assertTrue($map->toColumnType(type_integer())->isEqual(ColumnType::bigint()));
    }

    public function test_to_column_type_maps_json_to_jsonb(): void
    {
        $map = new EntryTypesMap();

        static::assertTrue($map->toColumnType(type_json())->isEqual(ColumnType::jsonb()));
    }

    public function test_to_column_type_maps_xml_to_xml(): void
    {
        $map = new EntryTypesMap();

        static::assertTrue($map->toColumnType(type_xml())->isEqual(ColumnType::xml()));
    }

    public function test_to_flow_type_maps_bigint_to_integer(): void
    {
        $map = new EntryTypesMap();

        static::assertInstanceOf(IntegerType::class, $map->toFlowType(ColumnType::bigint()));
    }

    public function test_to_flow_type_maps_jsonb_to_json(): void
    {
        $map = new EntryTypesMap();

        static::assertInstanceOf(JsonType::class, $map->toFlowType(ColumnType::jsonb()));
    }

    public function test_to_flow_type_maps_varchar_to_string(): void
    {
        $map = new EntryTypesMap();

        static::assertInstanceOf(StringType::class, $map->toFlowType(ColumnType::varchar(100)));
    }

    public function test_to_flow_type_throws_on_unsupported_type(): void
    {
        $this->expectException(TypeMappingException::class);

        (new EntryTypesMap())->toFlowType(ColumnType::custom('hstore'));
    }
}
