<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\Tests\Unit;

use DateInterval;
use DateTimeImmutable;
use DateTimeZone;
use Flow\ETL\Adapter\JSON\JSONEncoder;
use Flow\ETL\Row\TypedRowValues;
use Flow\ETL\Tests\Fixtures\Enum\BackedStringEnum;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Types\Value\Json;
use Flow\Types\Value\Uuid;
use stdClass;

use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_date;
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
use function Flow\Types\DSL\type_time_zone;
use function Flow\Types\DSL\type_uuid;

final class JSONEncoderTest extends FlowTestCase
{
    public function test_decode_passes_parsed_maps_through_unchanged(): void
    {
        static::assertSame(
            ['id' => 1, 'name' => 'Alice'],
            (new JSONEncoder())->decode([['id' => 1, 'name' => 'Alice']])[0]->values,
        );
    }

    public function test_encode_keeps_json_values_as_nested_structures(): void
    {
        static::assertSame(
            [['data' => ['a' => 1, 'b' => 2]]],
            (new JSONEncoder())->encode([
                new TypedRowValues(['data' => Json::fromArray(['a' => 1, 'b' => 2])], ['data' => type_json()]),
            ]),
        );
    }

    public function test_encode_passes_scalars_through(): void
    {
        static::assertSame(
            [['id' => 1, 'name' => 'Alice', 'active' => true, 'score' => 9.5, 'missing' => null]],
            (new JSONEncoder())->encode([
                new TypedRowValues([
                    'id' => 1,
                    'name' => 'Alice',
                    'active' => true,
                    'score' => 9.5,
                    'missing' => null,
                ], [
                    'id' => type_integer(),
                    'name' => type_string(),
                    'active' => type_boolean(),
                    'score' => type_float(),
                    'missing' => type_string(),
                ]),
            ]),
        );
    }

    public function test_encode_renders_date_with_the_date_format(): void
    {
        static::assertSame(
            [['at' => '2023-10-01']],
            (new JSONEncoder())->encode([
                new TypedRowValues(['at' => new DateTimeImmutable('2023-10-01 12:02:01 UTC')], ['at' => type_date()]),
            ]),
        );
    }

    public function test_encode_renders_datetime_with_the_datetime_format(): void
    {
        static::assertSame(
            [['at' => '2023-10-01T12:02:01+00:00']],
            (new JSONEncoder())->encode([
                new TypedRowValues(['at' => new DateTimeImmutable('2023-10-01 12:02:01 UTC')], [
                    'at' => type_datetime(),
                ]),
            ]),
        );
    }

    public function test_encode_renders_array_values_as_nested_arrays(): void
    {
        static::assertSame(
            [['tags' => [1, 2, 3]]],
            (new JSONEncoder())->encode([
                new TypedRowValues(['tags' => [1, 2, 3]], ['tags' => type_array()]),
            ]),
        );
    }

    public function test_encode_renders_list_of_structures_as_nested_arrays(): void
    {
        static::assertSame(
            [['tags' => [['t' => 'a'], ['t' => 'b']]]],
            (new JSONEncoder())->encode([
                new TypedRowValues(['tags' => [['t' => 'a'], ['t' => 'b']]], ['tags' => type_list(type_structure([
                    't' => type_string(),
                ]))]),
            ]),
        );
    }

    public function test_encode_renders_map_values_as_nested_arrays(): void
    {
        static::assertSame(
            [['translations' => ['en' => 'red', 'pl' => 'czerwony']]],
            (new JSONEncoder())->encode([
                new TypedRowValues(['translations' => ['en' => 'red', 'pl' => 'czerwony']], ['translations' => type_map(
                    type_string(),
                    type_string(),
                )]),
            ]),
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
            (new JSONEncoder())->encode([
                new TypedRowValues(['event' => [
                    'at' => new DateTimeImmutable('2023-10-01 12:02:01 UTC'),
                    'id' => new Uuid('f47ac10b-58cc-4372-a567-0e02b2c3d479'),
                    'payload' => Json::fromArray(['a' => 1]),
                    'level' => BackedStringEnum::one,
                ]], ['event' => type_structure([
                    'at' => type_datetime(),
                    'id' => type_uuid(),
                    'payload' => type_json(),
                    'level' => type_enum(BackedStringEnum::class),
                ])]),
            ]),
        );
    }

    public function test_encode_renders_nested_interval_float_and_bool_leaves(): void
    {
        static::assertSame(
            [['event' => ['took' => 3600000000, 'score' => 9.5, 'active' => true]]],
            (new JSONEncoder())->encode([
                new TypedRowValues(['event' => [
                    'took' => new DateInterval('PT1H'),
                    'score' => 9.5,
                    'active' => true,
                ]], ['event' => type_structure([
                    'took' => type_time(),
                    'score' => type_float(),
                    'active' => type_boolean(),
                ])]),
            ]),
        );
    }

    public function test_encode_renders_non_array_container_values_as_null(): void
    {
        static::assertSame(
            [['x' => null]],
            (new JSONEncoder())->encode([
                new TypedRowValues(['x' => new stdClass()], ['x' => type_list(type_string())]),
            ]),
        );
    }

    public function test_encode_renders_unrenderable_nested_values_as_null(): void
    {
        static::assertSame(
            [['event' => ['stream' => null]]],
            (new JSONEncoder())->encode([
                new TypedRowValues(['event' => ['stream' => new stdClass()]], ['event' => type_structure([
                    'stream' => type_string(),
                ])]),
            ]),
        );
    }

    public function test_encode_renders_time_as_microseconds(): void
    {
        static::assertSame(
            [['duration' => 3600000000]],
            (new JSONEncoder())->encode([
                new TypedRowValues(['duration' => new DateInterval('PT1H')], ['duration' => type_time()]),
            ]),
        );
    }

    public function test_encode_renders_a_timezone_as_its_iana_name(): void
    {
        static::assertSame(
            [['tz' => 'Europe/Warsaw']],
            (new JSONEncoder())->encode([
                new TypedRowValues(['tz' => new DateTimeZone('Europe/Warsaw')], ['tz' => type_time_zone()]),
            ]),
        );
    }

    public function test_encode_renders_uuid_as_string(): void
    {
        static::assertSame(
            [['id' => 'f47ac10b-58cc-4372-a567-0e02b2c3d479']],
            (new JSONEncoder())->encode([
                new TypedRowValues(['id' => new Uuid('f47ac10b-58cc-4372-a567-0e02b2c3d479')], ['id' => type_uuid()]),
            ]),
        );
    }
}
