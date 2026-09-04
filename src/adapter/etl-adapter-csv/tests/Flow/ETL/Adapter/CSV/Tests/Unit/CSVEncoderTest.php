<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\Tests\Unit;

use DateInterval;
use DateTimeImmutable;
use DateTimeZone;
use Flow\ETL\Adapter\CSV\CSVEncoder;
use Flow\ETL\Row\TypedRowValues;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Types\Value\Uuid;

use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_date;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_time;
use function Flow\Types\DSL\type_time_zone;
use function Flow\Types\DSL\type_uuid;

final class CSVEncoderTest extends FlowTestCase
{
    public function test_decode_captures_headers_from_the_first_row_and_emits_no_row_for_it(): void
    {
        $encoder = new CSVEncoder();

        static::assertSame([], $encoder->decode(['id,name']));
        static::assertSame(['id' => '1', 'name' => 'Norbert'], $encoder->decode(['1,Norbert'])[0]->values);
    }

    public function test_decode_generates_auto_headers_when_header_is_disabled(): void
    {
        static::assertSame(
            ['e00' => '1', 'e01' => 'Norbert'],
            (new CSVEncoder(withHeader: false))->decode(['1,Norbert'])[0]->values,
        );
    }

    public function test_decode_keeps_empty_fields_when_empty_to_null_is_disabled(): void
    {
        static::assertSame(
            ['id' => '1', 'name' => ''],
            (new CSVEncoder(emptyToNull: false))->decode(['id,name', '1,'])[0]->values,
        );
    }

    public function test_decode_maps_fields_to_headers(): void
    {
        static::assertSame(
            ['id' => '1', 'name' => 'Norbert'],
            (new CSVEncoder())->decode(['id,name', '1,Norbert'])[0]->values,
        );
    }

    public function test_decode_pads_short_rows_with_null(): void
    {
        static::assertSame(['id' => '1', 'name' => null], (new CSVEncoder())->decode(['id,name', '1'])[0]->values);
    }

    public function test_decode_truncates_rows_longer_than_the_headers(): void
    {
        static::assertSame(
            ['id' => '1', 'name' => 'Norbert'],
            (new CSVEncoder())->decode(['id,name', '1,Norbert,extra'])[0]->values,
        );
    }

    public function test_decode_turns_empty_fields_into_null_by_default(): void
    {
        static::assertSame(['id' => '1', 'name' => null], (new CSVEncoder())->decode(['id,name', '1,'])[0]->values);
    }

    public function test_encode_renders_array_values_as_json(): void
    {
        static::assertSame(
            ["\"[1,2,3]\"\n"],
            (new CSVEncoder(newLineSeparator: "\n"))->encode([
                new TypedRowValues(['tags' => [1, 2, 3]], ['tags' => type_array()]),
            ]),
        );
    }

    public function test_encode_renders_date_with_the_date_format(): void
    {
        static::assertSame(
            ["2023-10-01\n"],
            (new CSVEncoder(newLineSeparator: "\n"))->encode([
                new TypedRowValues(['at' => new DateTimeImmutable('2023-10-01 12:02:01 UTC')], ['at' => type_date()]),
            ]),
        );
    }

    public function test_encode_renders_datetime_with_the_datetime_format(): void
    {
        static::assertSame(
            ["2023-10-01T12:02:01+00:00\n"],
            (new CSVEncoder(newLineSeparator: "\n"))->encode([
                new TypedRowValues(['at' => new DateTimeImmutable('2023-10-01 12:02:01 UTC')], [
                    'at' => type_datetime(),
                ]),
            ]),
        );
    }

    public function test_encode_renders_null_as_an_empty_field(): void
    {
        static::assertSame(
            ["\n"],
            (new CSVEncoder(newLineSeparator: "\n"))->encode([
                new TypedRowValues(['name' => null], ['name' => type_string()]),
            ]),
        );
    }

    public function test_encode_renders_scalars_verbatim_in_value_order(): void
    {
        static::assertSame(
            ["9.99,1,Norbert\n"],
            (new CSVEncoder(newLineSeparator: "\n"))->encode([
                new TypedRowValues(['price' => 9.99, 'id' => 1, 'name' => 'Norbert'], [
                    'price' => type_float(),
                    'id' => type_integer(),
                    'name' => type_string(),
                ]),
            ]),
        );
    }

    public function test_encode_renders_time_as_microseconds(): void
    {
        static::assertSame(
            ["3600000000\n"],
            (new CSVEncoder(newLineSeparator: "\n"))->encode([
                new TypedRowValues(['duration' => new DateInterval('PT1H')], ['duration' => type_time()]),
            ]),
        );
    }

    public function test_encode_renders_a_timezone_as_its_iana_name(): void
    {
        static::assertSame(
            ["Europe/Warsaw\n"],
            (new CSVEncoder(newLineSeparator: "\n"))->encode([
                new TypedRowValues(['tz' => new DateTimeZone('Europe/Warsaw')], ['tz' => type_time_zone()]),
            ]),
        );
    }

    public function test_encode_renders_uuid_as_string(): void
    {
        static::assertSame(
            ["f47ac10b-58cc-4372-a567-0e02b2c3d479\n"],
            (new CSVEncoder(newLineSeparator: "\n"))->encode([
                new TypedRowValues(['id' => new Uuid('f47ac10b-58cc-4372-a567-0e02b2c3d479')], ['id' => type_uuid()]),
            ]),
        );
    }

    public function test_headers_are_null_before_any_line_is_decoded(): void
    {
        static::assertNull((new CSVEncoder())->headers());
    }

    public function test_the_generated_headers_are_exposed_without_a_header_line(): void
    {
        $encoder = new CSVEncoder(withHeader: false);
        $encoder->decode(['1,a']);

        static::assertSame(['e00', 'e01'], $encoder->headers());
    }

    public function test_the_resolved_headers_are_exposed(): void
    {
        $encoder = new CSVEncoder();

        static::assertSame([], $encoder->decode(['id,name']));
        static::assertSame(['id', 'name'], $encoder->headers());
    }

    public function test_an_empty_header_cell_is_named_by_position(): void
    {
        $encoder = new CSVEncoder();
        $encoder->decode([',name']);

        static::assertSame(['e00', 'name'], $encoder->headers());
    }
}
