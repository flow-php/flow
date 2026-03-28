<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql\Tests\Unit;

use function Flow\ETL\DSL\{bool_entry, datetime_entry, float_entry, int_entry, json_entry, list_entry, map_entry, str_entry, structure_entry, uuid_entry};
use function Flow\Types\DSL\{type_integer, type_list, type_map, type_string, type_structure};
use Flow\ETL\Adapter\PostgreSql\EntryTypesMap;
use Flow\ETL\Row\Entry\IntegerEntry;
use Flow\PostgreSql\Client\TypedValue;
use Flow\PostgreSql\Client\Types\ValueType;
use PHPUnit\Framework\TestCase;

final class EntryTypesMapTest extends TestCase
{
    public function test_allows_override_for_integer_entry_to_int2() : void
    {
        $map = new EntryTypesMap([
            IntegerEntry::class => ValueType::INT2,
        ]);

        $result = $map->mapEntry(int_entry('small_count', 42));

        self::assertInstanceOf(TypedValue::class, $result);
        self::assertSame(ValueType::INT2, $result->targetType);
        self::assertSame(42, $result->value);
    }

    public function test_maps_boolean_entry_to_bool_type() : void
    {
        $map = new EntryTypesMap();
        $result = $map->mapEntry(bool_entry('active', true));

        self::assertInstanceOf(TypedValue::class, $result);
        self::assertSame(ValueType::BOOL, $result->targetType);
        self::assertTrue($result->value);
    }

    public function test_maps_datetime_entry_to_timestamptz_type() : void
    {
        $map = new EntryTypesMap();
        $date = new \DateTimeImmutable('2024-01-15 10:30:00');
        $result = $map->mapEntry(datetime_entry('created_at', $date));

        self::assertInstanceOf(TypedValue::class, $result);
        self::assertSame(ValueType::TIMESTAMPTZ, $result->targetType);
        self::assertEquals($date, $result->value);
    }

    public function test_maps_float_entry_to_float8_type() : void
    {
        $map = new EntryTypesMap();
        $result = $map->mapEntry(float_entry('price', 99.99));

        self::assertInstanceOf(TypedValue::class, $result);
        self::assertSame(ValueType::FLOAT8, $result->targetType);
        self::assertSame(99.99, $result->value);
    }

    public function test_maps_integer_entry_to_int8_type() : void
    {
        $map = new EntryTypesMap();
        $result = $map->mapEntry(int_entry('count', 42));

        self::assertInstanceOf(TypedValue::class, $result);
        self::assertSame(ValueType::INT8, $result->targetType);
        self::assertSame(42, $result->value);
    }

    public function test_maps_json_entry_to_jsonb_type() : void
    {
        $map = new EntryTypesMap();
        $result = $map->mapEntry(json_entry('data', ['key' => 'value']));

        self::assertInstanceOf(TypedValue::class, $result);
        self::assertSame(ValueType::JSONB, $result->targetType);
    }

    public function test_maps_list_entry_to_jsonb_type() : void
    {
        $map = new EntryTypesMap();
        $result = $map->mapEntry(list_entry('tags', [1, 2, 3], type_list(type_integer())));

        self::assertInstanceOf(TypedValue::class, $result);
        self::assertSame(ValueType::JSONB, $result->targetType);
        self::assertSame([1, 2, 3], $result->value);
    }

    public function test_maps_map_entry_to_jsonb_type() : void
    {
        $map = new EntryTypesMap();
        $result = $map->mapEntry(map_entry('metadata', ['key' => 'value'], type_map(type_string(), type_string())));

        self::assertInstanceOf(TypedValue::class, $result);
        self::assertSame(ValueType::JSONB, $result->targetType);
        self::assertSame(['key' => 'value'], $result->value);
    }

    public function test_maps_null_value_to_null() : void
    {
        $map = new EntryTypesMap();
        $result = $map->mapEntry(str_entry('name', null));

        self::assertNull($result);
    }

    public function test_maps_string_entry_to_text_type() : void
    {
        $map = new EntryTypesMap();
        $result = $map->mapEntry(str_entry('name', 'Alice'));

        self::assertInstanceOf(TypedValue::class, $result);
        self::assertSame(ValueType::TEXT, $result->targetType);
        self::assertSame('Alice', $result->value);
    }

    public function test_maps_structure_entry_to_jsonb_type() : void
    {
        $map = new EntryTypesMap();
        $result = $map->mapEntry(structure_entry('user', ['name' => 'Alice', 'age' => 30], type_structure([
            'name' => type_string(),
            'age' => type_integer(),
        ])));

        self::assertInstanceOf(TypedValue::class, $result);
        self::assertSame(ValueType::JSONB, $result->targetType);
    }

    public function test_maps_uuid_entry_to_uuid_type() : void
    {
        $map = new EntryTypesMap();
        $result = $map->mapEntry(uuid_entry('id', '550e8400-e29b-41d4-a716-446655440000'));

        self::assertInstanceOf(TypedValue::class, $result);
        self::assertSame(ValueType::UUID, $result->targetType);
    }
}
