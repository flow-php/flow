<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Unit;

use DateInterval;
use DateTimeImmutable;
use Flow\ETL\Adapter\Parquet\ValueHydrator;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Types\Value\Json;
use Flow\Types\Value\Uuid;

use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\date_schema;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\json_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\map_schema;
use function Flow\ETL\DSL\string_schema;
use function Flow\ETL\DSL\structure_schema;
use function Flow\ETL\DSL\time_schema;
use function Flow\ETL\DSL\uuid_schema;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;

final class ValueHydratorTest extends FlowTestCase
{
    public function test_hydrating_already_hydrated_values_returns_them_as_is(): void
    {
        $json = new Json('{"a":1}');
        $uuid = new Uuid('00000000-0000-0000-0000-000000000000');

        static::assertSame($json, (new ValueHydrator())->hydrate($json, json_schema('json')));
        static::assertSame($uuid, (new ValueHydrator())->hydrate($uuid, uuid_schema('uuid')));
    }

    public function test_hydrating_json_string_into_json_value(): void
    {
        static::assertEquals(
            new Json('{"id":1,"name":"flow"}'),
            (new ValueHydrator())->hydrate('{"id":1,"name":"flow"}', json_schema('json')),
        );
    }

    public function test_hydrating_json_string_into_json_value_for_nullable_definition(): void
    {
        static::assertEquals(
            new Json('{"id":1}'),
            (new ValueHydrator())->hydrate('{"id":1}', json_schema('json', true)),
        );
    }

    public function test_hydrating_null(): void
    {
        static::assertNull((new ValueHydrator())->hydrate(null, int_schema('int', true)));
        static::assertNull((new ValueHydrator())->hydrate(null, json_schema('json', true)));
        static::assertNull((new ValueHydrator())->hydrate(null, uuid_schema('uuid', true)));
        static::assertNull((new ValueHydrator())->hydrate(null, int_schema('int')));
    }

    public function test_hydrating_uuid_string_into_uuid_value(): void
    {
        static::assertEquals(
            new Uuid('f47ac10b-58cc-4372-a567-0e02b2c3d479'),
            (new ValueHydrator())->hydrate('f47ac10b-58cc-4372-a567-0e02b2c3d479', uuid_schema('uuid')),
        );
    }

    public function test_hydrating_uuid_string_into_uuid_value_for_nullable_definition(): void
    {
        static::assertEquals(
            new Uuid('f47ac10b-58cc-4372-a567-0e02b2c3d479'),
            (new ValueHydrator())->hydrate('f47ac10b-58cc-4372-a567-0e02b2c3d479', uuid_schema('uuid', true)),
        );
    }

    public function test_values_of_types_without_flow_value_objects_are_passed_through_without_casting(): void
    {
        $datetime = new DateTimeImmutable('2024-04-01 10:00:00 UTC');
        $time = new DateInterval('PT2H30M');
        $list = [1, 2, 3];
        $map = ['a' => 1, 'b' => 2];
        $structure = ['lat' => 1.5, 'lon' => 2.5];

        static::assertSame(1, (new ValueHydrator())->hydrate(1, int_schema('int')));
        static::assertSame(1.5, (new ValueHydrator())->hydrate(1.5, float_schema('float')));
        static::assertTrue((new ValueHydrator())->hydrate(true, bool_schema('bool')));
        static::assertSame('flow', (new ValueHydrator())->hydrate('flow', string_schema('string')));
        static::assertSame($datetime, (new ValueHydrator())->hydrate($datetime, datetime_schema('datetime')));
        static::assertSame($datetime, (new ValueHydrator())->hydrate($datetime, date_schema('date')));
        static::assertSame($time, (new ValueHydrator())->hydrate($time, time_schema('time')));
        static::assertSame($list, (new ValueHydrator())->hydrate($list, list_schema(
            'list',
            type_list(type_integer()),
        )));
        static::assertSame($map, (new ValueHydrator())->hydrate($map, map_schema('map', type_map(
            type_string(),
            type_integer(),
        ))));
        static::assertSame($structure, (new ValueHydrator())->hydrate($structure, structure_schema('struct', type_structure([
            'lat' => type_float(),
            'lon' => type_float(),
        ]))));
    }
}
