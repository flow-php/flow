<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\Tests\Unit;

use DateInterval;
use DateTimeImmutable;
use DateTimeZone;
use Flow\ETL\Adapter\JSON\JSONEncoder;
use Flow\ETL\Tests\Fixtures\Enum\BackedStringEnum;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Types\Value\Json;
use Flow\Types\Value\Uuid;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\date_schema;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\json_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\map_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\structure_schema;
use function Flow\ETL\DSL\time_schema;
use function Flow\ETL\DSL\time_zone_schema;
use function Flow\ETL\DSL\uuid_schema;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_enum;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_json;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function Flow\Types\DSL\type_time;
use function Flow\Types\DSL\type_uuid;

final class JSONEncoderTest extends FlowTestCase
{
    public function test_encode_keeps_json_values_as_nested_structures(): void
    {
        static::assertSame(
            [['data' => ['a' => 1, 'b' => 2]]],
            (new JSONEncoder())->encode(array_to_rows([['data' => Json::fromArray([
                'a' => 1,
                'b' => 2,
            ])]], schema(json_schema('data')))),
        );
    }

    public function test_encode_passes_scalars_through(): void
    {
        static::assertSame(
            [['id' => 1, 'name' => 'Alice', 'active' => true, 'score' => 9.5, 'missing' => null]],
            (new JSONEncoder())->encode(array_to_rows(
                [['id' => 1, 'name' => 'Alice', 'active' => true, 'score' => 9.5, 'missing' => null]],
                schema(
                    int_schema('id'),
                    str_schema('name'),
                    bool_schema('active'),
                    float_schema('score'),
                    str_schema('missing', nullable: true),
                ),
            )),
        );
    }

    public function test_encode_renders_date_with_the_date_format(): void
    {
        static::assertSame(
            [['at' => '2023-10-01']],
            (new JSONEncoder())->encode(array_to_rows([[
                'at' => new DateTimeImmutable('2023-10-01 12:02:01 UTC'),
            ]], schema(date_schema('at')))),
        );
    }

    public function test_encode_renders_datetime_with_the_datetime_format(): void
    {
        static::assertSame(
            [['at' => '2023-10-01T12:02:01+00:00']],
            (new JSONEncoder())->encode(array_to_rows([[
                'at' => new DateTimeImmutable('2023-10-01 12:02:01 UTC'),
            ]], schema(datetime_schema('at')))),
        );
    }

    public function test_encode_renders_list_values_as_nested_arrays(): void
    {
        static::assertSame(
            [['tags' => [1, 2, 3]]],
            (new JSONEncoder())->encode(array_to_rows([['tags' => [
                1,
                2,
                3,
            ]]], schema(list_schema('tags', type_list(type_integer()))))),
        );
    }

    public function test_encode_renders_list_of_structures_as_nested_arrays(): void
    {
        static::assertSame(
            [['tags' => [['t' => 'a'], ['t' => 'b']]]],
            (new JSONEncoder())->encode(array_to_rows([[
                'tags' => [['t' => 'a'], ['t' => 'b']],
            ]], schema(list_schema('tags', type_list(type_structure(['t' => type_string()])))))),
        );
    }

    public function test_encode_renders_map_values_as_nested_arrays(): void
    {
        static::assertSame(
            [['translations' => ['en' => 'red', 'pl' => 'czerwony']]],
            (new JSONEncoder())->encode(array_to_rows([['translations' => [
                'en' => 'red',
                'pl' => 'czerwony',
            ]]], schema(map_schema('translations', type_map(type_string(), type_string()))))),
        );
    }

    public function test_encode_renders_nested_datetime_uuid_json_and_enum_leaves(): void
    {
        static::assertSame(
            [[
                'event' => [
                    'at' => '2023-10-01T12:02:01+00:00',
                    'id' => 'f47ac10b-58cc-4372-a567-0e02b2c3d479',
                    'payload' => ['a' => 1],
                    'level' => 'one',
                ],
            ]],
            (new JSONEncoder())->encode(array_to_rows([[
                'event' => [
                    'at' => new DateTimeImmutable('2023-10-01 12:02:01 UTC'),
                    'id' => new Uuid('f47ac10b-58cc-4372-a567-0e02b2c3d479'),
                    'payload' => Json::fromArray(['a' => 1]),
                    'level' => BackedStringEnum::one,
                ],
            ]], schema(structure_schema('event', type_structure([
                'at' => type_datetime(),
                'id' => type_uuid(),
                'payload' => type_json(),
                'level' => type_enum(BackedStringEnum::class),
            ]))))),
        );
    }

    public function test_encode_renders_nested_interval_float_and_bool_leaves(): void
    {
        static::assertSame(
            [['event' => ['took' => 3600000000, 'score' => 9.5, 'active' => true]]],
            (new JSONEncoder())->encode(array_to_rows([['event' => [
                'took' => new DateInterval('PT1H'),
                'score' => 9.5,
                'active' => true,
            ]]], schema(structure_schema('event', type_structure([
                'took' => type_time(),
                'score' => type_float(),
                'active' => type_boolean(),
            ]))))),
        );
    }

    public function test_encode_renders_time_as_microseconds(): void
    {
        static::assertSame(
            [['duration' => 3600000000]],
            (new JSONEncoder())->encode(array_to_rows([[
                'duration' => new DateInterval('PT1H'),
            ]], schema(time_schema('duration')))),
        );
    }

    public function test_encode_renders_a_timezone_as_its_iana_name(): void
    {
        static::assertSame(
            [['tz' => 'Europe/Warsaw']],
            (new JSONEncoder())->encode(array_to_rows([[
                'tz' => new DateTimeZone('Europe/Warsaw'),
            ]], schema(time_zone_schema('tz')))),
        );
    }

    public function test_encode_renders_uuid_as_string(): void
    {
        static::assertSame(
            [['id' => 'f47ac10b-58cc-4372-a567-0e02b2c3d479']],
            (new JSONEncoder())->encode(array_to_rows([[
                'id' => new Uuid('f47ac10b-58cc-4372-a567-0e02b2c3d479'),
            ]], schema(uuid_schema('id')))),
        );
    }
}
