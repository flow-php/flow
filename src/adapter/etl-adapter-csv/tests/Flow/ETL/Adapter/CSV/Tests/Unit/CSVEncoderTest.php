<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\Tests\Unit;

use DateInterval;
use DateTimeImmutable;
use DateTimeZone;
use Flow\ETL\Adapter\CSV\CSVEncoder;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Types\Value\Uuid;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\date_schema;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\time_schema;
use function Flow\ETL\DSL\time_zone_schema;
use function Flow\ETL\DSL\uuid_schema;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;

final class CSVEncoderTest extends FlowTestCase
{
    public function test_encode_renders_list_values_as_json(): void
    {
        static::assertSame(
            ["\"[1,2,3]\"\n"],
            (new CSVEncoder(newLineSeparator: "\n"))->encode(array_to_rows([['tags' => [
                1,
                2,
                3,
            ]]], schema(list_schema('tags', type_list(type_integer()))))),
        );
    }

    public function test_encode_renders_date_with_the_date_format(): void
    {
        static::assertSame(
            ["2023-10-01\n"],
            (new CSVEncoder(newLineSeparator: "\n"))->encode(array_to_rows([[
                'at' => new DateTimeImmutable('2023-10-01 12:02:01 UTC'),
            ]], schema(date_schema('at')))),
        );
    }

    public function test_encode_renders_datetime_with_the_datetime_format(): void
    {
        static::assertSame(
            ["2023-10-01T12:02:01+00:00\n"],
            (new CSVEncoder(newLineSeparator: "\n"))->encode(array_to_rows([[
                'at' => new DateTimeImmutable('2023-10-01 12:02:01 UTC'),
            ]], schema(datetime_schema('at')))),
        );
    }

    public function test_encode_renders_null_as_an_empty_field(): void
    {
        static::assertSame(
            ["\n"],
            (new CSVEncoder(newLineSeparator: "\n"))->encode(array_to_rows([[
                'name' => null,
            ]], schema(str_schema('name', nullable: true)))),
        );
    }

    public function test_encode_renders_scalars_verbatim_in_value_order(): void
    {
        static::assertSame(
            ["9.99,1,Norbert\n"],
            (new CSVEncoder(newLineSeparator: "\n"))->encode(array_to_rows(
                [['price' => 9.99, 'id' => 1, 'name' => 'Norbert']],
                schema(float_schema('price'), int_schema('id'), str_schema('name')),
            )),
        );
    }

    public function test_encode_renders_time_as_microseconds(): void
    {
        static::assertSame(
            ["3600000000\n"],
            (new CSVEncoder(newLineSeparator: "\n"))->encode(array_to_rows([[
                'duration' => new DateInterval('PT1H'),
            ]], schema(time_schema('duration')))),
        );
    }

    public function test_encode_renders_a_timezone_as_its_iana_name(): void
    {
        static::assertSame(
            ["Europe/Warsaw\n"],
            (new CSVEncoder(newLineSeparator: "\n"))->encode(array_to_rows([[
                'tz' => new DateTimeZone('Europe/Warsaw'),
            ]], schema(time_zone_schema('tz')))),
        );
    }

    public function test_encode_renders_uuid_as_string(): void
    {
        static::assertSame(
            ["f47ac10b-58cc-4372-a567-0e02b2c3d479\n"],
            (new CSVEncoder(newLineSeparator: "\n"))->encode(array_to_rows([[
                'id' => new Uuid('f47ac10b-58cc-4372-a567-0e02b2c3d479'),
            ]], schema(uuid_schema('id')))),
        );
    }
}
