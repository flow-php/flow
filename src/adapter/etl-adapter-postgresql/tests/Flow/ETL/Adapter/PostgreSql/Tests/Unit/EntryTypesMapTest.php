<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql\Tests\Unit;

use DateTimeImmutable;
use Flow\ETL\Adapter\PostgreSql\EntryTypesMap;
use Flow\ETL\Adapter\PostgreSql\Exception\TypeMappingException;
use Flow\ETL\Row\Entry\IntegerEntry;
use Flow\PostgreSql\Client\TypedValue;
use Flow\PostgreSql\Client\Types\ValueType;
use Flow\PostgreSql\QueryBuilder\Schema\ColumnType;
use Flow\Types\Type\Logical\JsonType;
use Flow\Types\Type\Native\IntegerType;
use Flow\Types\Type\Native\StringType;
use PHPUnit\Framework\TestCase;

use function Flow\ETL\DSL\bool_entry;
use function Flow\ETL\DSL\datetime_entry;
use function Flow\ETL\DSL\float_entry;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\json_entry;
use function Flow\ETL\DSL\list_entry;
use function Flow\ETL\DSL\map_entry;
use function Flow\ETL\DSL\str_entry;
use function Flow\ETL\DSL\structure_entry;
use function Flow\ETL\DSL\uuid_entry;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_json;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function Flow\Types\DSL\type_xml;

final class EntryTypesMapTest extends TestCase
{
    public function test_allows_override_for_integer_entry_to_int2(): void
    {
        $map = new EntryTypesMap([
            IntegerEntry::class => ValueType::INT2,
        ]);

        $result = $map->mapEntry(int_entry('small_count', 42));

        static::assertInstanceOf(TypedValue::class, $result);
        static::assertSame(ValueType::INT2, $result->targetType);
        static::assertSame(42, $result->value);
    }

    public function test_maps_boolean_entry_to_bool_type(): void
    {
        $map = new EntryTypesMap();
        $result = $map->mapEntry(bool_entry('active', true));

        static::assertInstanceOf(TypedValue::class, $result);
        static::assertSame(ValueType::BOOL, $result->targetType);
        static::assertTrue($result->value);
    }

    public function test_maps_datetime_entry_to_timestamptz_type(): void
    {
        $map = new EntryTypesMap();
        $date = new DateTimeImmutable('2024-01-15 10:30:00');
        $result = $map->mapEntry(datetime_entry('created_at', $date));

        static::assertInstanceOf(TypedValue::class, $result);
        static::assertSame(ValueType::TIMESTAMPTZ, $result->targetType);
        static::assertEquals($date, $result->value);
    }

    public function test_maps_float_entry_to_float8_type(): void
    {
        $map = new EntryTypesMap();
        $result = $map->mapEntry(float_entry('price', 99.99));

        static::assertInstanceOf(TypedValue::class, $result);
        static::assertSame(ValueType::FLOAT8, $result->targetType);
        static::assertSame(99.99, $result->value);
    }

    public function test_maps_integer_entry_to_int8_type(): void
    {
        $map = new EntryTypesMap();
        $result = $map->mapEntry(int_entry('count', 42));

        static::assertInstanceOf(TypedValue::class, $result);
        static::assertSame(ValueType::INT8, $result->targetType);
        static::assertSame(42, $result->value);
    }

    public function test_maps_json_entry_to_jsonb_type(): void
    {
        $map = new EntryTypesMap();
        $result = $map->mapEntry(json_entry('data', ['key' => 'value']));

        static::assertInstanceOf(TypedValue::class, $result);
        static::assertSame(ValueType::JSONB, $result->targetType);
    }

    public function test_maps_list_entry_to_jsonb_type(): void
    {
        $map = new EntryTypesMap();
        $result = $map->mapEntry(list_entry('tags', [1, 2, 3], type_list(type_integer())));

        static::assertInstanceOf(TypedValue::class, $result);
        static::assertSame(ValueType::JSONB, $result->targetType);
        static::assertSame([1, 2, 3], $result->value);
    }

    public function test_maps_map_entry_to_jsonb_type(): void
    {
        $map = new EntryTypesMap();
        $result = $map->mapEntry(map_entry('metadata', ['key' => 'value'], type_map(type_string(), type_string())));

        static::assertInstanceOf(TypedValue::class, $result);
        static::assertSame(ValueType::JSONB, $result->targetType);
        static::assertSame(['key' => 'value'], $result->value);
    }

    public function test_maps_null_value_to_null(): void
    {
        $map = new EntryTypesMap();
        $result = $map->mapEntry(str_entry('name', null));

        static::assertNull($result);
    }

    public function test_maps_string_entry_to_text_type(): void
    {
        $map = new EntryTypesMap();
        $result = $map->mapEntry(str_entry('name', 'Alice'));

        static::assertInstanceOf(TypedValue::class, $result);
        static::assertSame(ValueType::TEXT, $result->targetType);
        static::assertSame('Alice', $result->value);
    }

    public function test_maps_structure_entry_to_jsonb_type(): void
    {
        $map = new EntryTypesMap();
        $result = $map->mapEntry(structure_entry('user', ['name' => 'Alice', 'age' => 30], type_structure([
            'name' => type_string(),
            'age' => type_integer(),
        ])));

        static::assertInstanceOf(TypedValue::class, $result);
        static::assertSame(ValueType::JSONB, $result->targetType);
    }

    public function test_maps_uuid_entry_to_uuid_type(): void
    {
        $map = new EntryTypesMap();
        $result = $map->mapEntry(uuid_entry('id', '550e8400-e29b-41d4-a716-446655440000'));

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
