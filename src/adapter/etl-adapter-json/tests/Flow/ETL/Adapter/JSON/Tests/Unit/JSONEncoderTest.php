<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\Tests\Unit;

use DateInterval;
use DateTimeImmutable;
use Flow\ETL\Adapter\JSON\JSONEncoder;
use Flow\ETL\Row\TypedRowValues;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Types\Value\Json;
use Flow\Types\Value\Uuid;

use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_date;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_json;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_time;
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

    public function test_encode_renders_list_map_structure_values_as_json_strings(): void
    {
        static::assertSame(
            [['tags' => '[1,2,3]']],
            (new JSONEncoder())->encode([
                new TypedRowValues(['tags' => [1, 2, 3]], ['tags' => type_array()]),
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
