<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql\Tests\Unit;

use DateTimeImmutable;
use DateTimeZone;
use Flow\ETL\Adapter\PostgreSql\EntryTypesMap;
use Flow\ETL\Adapter\PostgreSql\Exception\TypeMappingException;
use Flow\PostgreSql\Client\TypedValue;
use Flow\PostgreSql\Client\Types\ResultCaster;
use Flow\PostgreSql\Client\Types\ValueType;
use Flow\PostgreSql\QueryBuilder\Schema\ColumnType;
use Flow\Types\Type;
use Flow\Types\Type\Logical\JsonType;
use Flow\Types\Type\Native\IntegerType;
use Flow\Types\Type\Native\StringType;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

use function Flow\PostgreSql\DSL\column_type_from_string;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_date;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_json;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_null;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function Flow\Types\DSL\type_time;
use function Flow\Types\DSL\type_time_zone;
use function Flow\Types\DSL\type_union;
use function Flow\Types\DSL\type_uuid;
use function Flow\Types\DSL\type_xml;

final class EntryTypesMapTest extends TestCase
{
    /**
     * The exact Flow type each pg name maps to.
     *
     * @return Generator<string, array{string, Type<mixed>}>
     */
    public static function provide_mapped_types_with_their_flow_type(): Generator
    {
        yield 'int2' => ['int2', type_integer()];
        yield 'int4' => ['int4', type_integer()];
        yield 'int8' => ['int8', type_integer()];
        yield 'oid' => ['oid', type_integer()];
        yield 'float4' => ['float4', type_float()];
        yield 'float8' => ['float8', type_float()];
        yield 'numeric' => ['numeric', type_float()];
        yield 'bool' => ['bool', type_boolean()];
        yield 'text' => ['text', type_string()];
        yield 'varchar' => ['varchar', type_string()];
        yield 'bpchar' => ['bpchar', type_string()];
        yield 'bytea' => ['bytea', type_string()];
        yield 'date' => ['date', type_date()];
        yield 'time' => ['time', type_time()];
        yield 'timetz' => ['timetz', type_time()];
        yield 'timestamp' => ['timestamp', type_datetime()];
        yield 'timestamptz' => ['timestamptz', type_datetime()];
        yield 'uuid' => ['uuid', type_uuid()];
        yield 'json' => ['json', type_json()];
        yield 'jsonb' => ['jsonb', type_json()];
        yield 'xml' => ['xml', type_xml()];
        yield 'inet' => ['inet', type_string()];
        yield 'cidr' => ['cidr', type_string()];
        yield 'macaddr' => ['macaddr', type_string()];
        yield 'money' => ['money', type_string()];
        yield 'int4range' => ['int4range', type_string()];
        yield 'int8range' => ['int8range', type_string()];
        yield 'numrange' => ['numrange', type_string()];
        yield 'tsrange' => ['tsrange', type_string()];
        yield 'tstzrange' => ['tstzrange', type_string()];
        yield 'daterange' => ['daterange', type_string()];
        yield 'interval takes the text floor' => ['interval', type_string()];
    }

    /**
     * Every pg name the schema map accepts, with a literal in the wire form pg hands to PHP.
     *
     * @return Generator<string, array{string, string}>
     */
    public static function provide_mapped_types_with_a_wire_value(): Generator
    {
        yield 'int2' => ['int2', '42'];
        yield 'int4' => ['int4', '42'];
        yield 'int8' => ['int8', '42'];
        yield 'oid' => ['oid', '42'];
        yield 'float4' => ['float4', '1.5'];
        yield 'float8' => ['float8', '1.5'];
        yield 'numeric' => ['numeric', '10.50'];
        yield 'bool' => ['bool', 't'];
        yield 'text' => ['text', 'abc'];
        yield 'varchar' => ['varchar', 'abc'];
        yield 'bpchar' => ['bpchar', 'abc'];
        yield 'bytea' => ['bytea', '\x68656c6c6f'];
        yield 'date' => ['date', '2026-01-01'];
        yield 'time' => ['time', '12:34:56'];
        yield 'timetz' => ['timetz', '12:34:56+02'];
        yield 'interval' => ['interval', '1 day 01:30:00'];
        yield 'timestamp' => ['timestamp', '2026-01-01 10:00:00'];
        yield 'timestamptz' => ['timestamptz', '2026-01-01 10:00:00+00:00'];
        yield 'uuid' => ['uuid', '123e4567-e89b-12d3-a456-426614174000'];
        yield 'json' => ['json', '{"a":1}'];
        yield 'jsonb' => ['jsonb', '{"a":1}'];
        yield 'xml' => ['xml', '<a>b</a>'];
        yield 'inet' => ['inet', '192.168.1.1'];
        yield 'cidr' => ['cidr', '10.0.0.0/8'];
        yield 'macaddr' => ['macaddr', '08:00:2b:01:02:03'];
        yield 'money' => ['money', '$12.50'];
        yield 'int4range' => ['int4range', '[1,10)'];
        yield 'daterange' => ['daterange', '[2026-01-01,2026-02-01)'];
        yield 'a pg enum reports its own name' => ['mood', 'happy'];
        yield '_int4' => ['_int4', '{1,2}'];
        yield '_text' => ['_text', '{a,b}'];
        yield '_bool' => ['_bool', '{t,f}'];
        yield '_text with a NULL element' => ['_text', '{NULL,a}'];
        yield '_int4 with a NULL element' => ['_int4', '{1,NULL}'];
    }

    /**
     * @return Generator<string, array{string}>
     */
    public static function provide_unmapped_types(): Generator
    {
        foreach (['record', 'point', 'line', 'lseg', 'box', 'path', 'polygon', 'circle'] as $type) {
            yield $type => [$type];
        }
    }

    /**
     * @param Type<mixed> $expected
     */
    #[DataProvider('provide_mapped_types_with_their_flow_type')]
    public function test_every_mapped_postgresql_type_becomes_its_flow_type(string $pgType, Type $expected): void
    {
        static::assertEquals(
            $expected,
            (new EntryTypesMap())->toFlowTypeWithTextFloor(column_type_from_string($pgType)),
        );
    }

    public function test_an_unrecognised_name_is_refused_on_the_catalog_route(): void
    {
        // The text floor is only legitimate where the caster leaves the value as text, which is the
        // result route. SchemaConverter reads the catalog and must keep refusing, so the user is told
        // to map the type explicitly instead of silently receiving a string column.
        $this->expectException(TypeMappingException::class);

        (new EntryTypesMap())->toFlowType(column_type_from_string('hstore'));
    }

    public function test_an_unrecognised_name_takes_the_text_floor_on_the_result_route(): void
    {
        static::assertEquals(
            type_string(),
            (new EntryTypesMap())->toFlowTypeWithTextFloor(column_type_from_string('hstore')),
        );
    }

    #[DataProvider('provide_mapped_types_with_a_wire_value')]
    public function test_every_mapped_type_has_a_caster_arm(string $pgType, string $wireValue): void
    {
        // Map subset of arms, not pairwise: a mapping is only legitimate while the caster produces
        // a value the derived schema accepts.
        $flowType = (new EntryTypesMap())->toFlowTypeWithTextFloor(column_type_from_string($pgType));

        static::assertTrue($flowType->isValid($flowType->cast((new ResultCaster())->cast($wireValue, $pgType))));
    }

    #[DataProvider('provide_unmapped_types')]
    public function test_record_and_the_geometric_types_are_refused(string $pgType): void
    {
        $this->expectException(TypeMappingException::class);

        (new EntryTypesMap())->toFlowTypeWithTextFloor(column_type_from_string($pgType));
    }

    public function test_an_array_of_an_unmapped_element_type_is_refused(): void
    {
        $this->expectException(TypeMappingException::class);

        (new EntryTypesMap())->toFlowTypeWithTextFloor(column_type_from_string('_point'));
    }

    public function test_an_array_type_becomes_a_list_of_its_element_type(): void
    {
        $map = new EntryTypesMap();

        // A pg array may hold SQL NULLs, which the caster preserves, so the element type has to
        // admit them or the read corrupts them.
        static::assertEquals(
            type_list(type_union(type_integer(), type_null())),
            $map->toFlowTypeWithTextFloor(column_type_from_string('_int4')),
        );
        static::assertEquals(
            type_list(type_union(type_string(), type_null())),
            $map->toFlowTypeWithTextFloor(column_type_from_string('_text')),
        );
        static::assertEquals(
            type_list(type_union(type_datetime(), type_null())),
            $map->toFlowTypeWithTextFloor(column_type_from_string('_timestamptz')),
        );
    }

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

    public function test_maps_time_zone_type_to_text(): void
    {
        $result = (new EntryTypesMap())->map('tz', type_time_zone(), new DateTimeZone('Europe/Warsaw'));

        static::assertInstanceOf(TypedValue::class, $result);
        static::assertSame(ValueType::TEXT, $result->targetType);
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

    public function test_to_flow_type_floors_an_unknown_type_name_to_string(): void
    {
        // The result route cannot ask the catalog whether an unknown name is an enum, a domain or
        // an extension type, and pg transmits all of them as text, so they take the string floor.
        // Only record and the geometric types still throw - see the test below.
        static::assertEquals(
            type_string(),
            (new EntryTypesMap())->toFlowTypeWithTextFloor(ColumnType::custom('hstore')),
        );
    }
}
