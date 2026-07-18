<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel\Tests\Unit;

use DateInterval;
use DateTimeImmutable;
use Flow\ETL\Adapter\Excel\ExcelEncoder;
use Flow\ETL\Row\TypedRowValues;
use Flow\ETL\Tests\Fixtures\Enum\BackedIntEnum;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Types\Value\Uuid;

use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_date;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_enum;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_time;
use function Flow\Types\DSL\type_uuid;

final class ExcelEncoderTest extends FlowTestCase
{
    public function test_decode_captures_headers_from_the_first_row_and_emits_no_row_for_it(): void
    {
        $encoder = new ExcelEncoder();

        static::assertSame([], $encoder->decode([['id', 'name']]));
        static::assertSame(['id' => 1, 'name' => 'Norbert'], $encoder->decode([[1, 'Norbert']])[0]->values);
    }

    public function test_decode_generates_auto_headers_when_header_is_disabled(): void
    {
        static::assertSame(
            ['e00' => 1, 'e01' => 'Norbert'],
            (new ExcelEncoder(withHeader: false))->decode([[1, 'Norbert']])[0]->values,
        );
    }

    public function test_decode_keeps_empty_cells_when_convert_empty_to_null_is_disabled(): void
    {
        static::assertSame(
            ['id' => 1, 'name' => ''],
            (new ExcelEncoder(convertEmptyToNull: false))->decode([['id', 'name'], [1, '']])[0]->values,
        );
    }

    public function test_decode_maps_cells_to_headers(): void
    {
        static::assertSame(
            ['id' => 1, 'name' => 'Norbert'],
            (new ExcelEncoder())->decode([['id', 'name'], [1, 'Norbert']])[0]->values,
        );
    }

    public function test_decode_turns_empty_cells_into_null_by_default(): void
    {
        static::assertSame(
            ['id' => 1, 'name' => null],
            (new ExcelEncoder())->decode([['id', 'name'], [1, '']])[0]->values,
        );
    }

    public function test_encodes_scalars_as_a_flat_cell_list_in_value_order(): void
    {
        static::assertSame(
            [[1, 'Norbert', 1.5, true]],
            (new ExcelEncoder())->encode([new TypedRowValues([
                'id' => 1,
                'name' => 'Norbert',
                'p' => 1.5,
                'a' => true,
            ], ['id' => type_integer(), 'name' => type_string(), 'p' => type_float(), 'a' => type_boolean()])]),
        );
    }

    public function test_encodes_dates_with_the_date_format_datetimes_with_the_datetime_format_and_intervals_with_the_time_format(): void
    {
        $encoder = new ExcelEncoder(dateTimeFormat: 'd/m/Y H:i', timeFormat: '%H:%I');

        static::assertSame(
            [['2024-08-01', '01/08/2024 10:30', '01:30']],
            $encoder->encode([new TypedRowValues([
                'd' => new DateTimeImmutable('2024-08-01'),
                'dt' => new DateTimeImmutable('2024-08-01 10:30:00'),
                't' => new DateInterval('PT1H30M'),
            ], ['d' => type_date(), 'dt' => type_datetime(), 't' => type_time()])]),
        );
    }

    public function test_encodes_time_with_the_default_time_format(): void
    {
        static::assertSame(
            [['14:30:45']],
            (new ExcelEncoder())->encode([
                new TypedRowValues(['t' => new DateInterval('PT14H30M45S')], ['t' => type_time()]),
            ]),
        );
    }

    public function test_encodes_backed_enum_as_its_backing_value(): void
    {
        static::assertSame(
            [['1']],
            (new ExcelEncoder())->encode([
                new TypedRowValues(['e' => BackedIntEnum::one], ['e' => type_enum(BackedIntEnum::class)]),
            ]),
        );
    }

    public function test_encodes_arrays_as_json(): void
    {
        static::assertSame(
            [['["a","b"]', '{"x":1}', '{"city":"Krakow"}']],
            (new ExcelEncoder())->encode([new TypedRowValues([
                'tags' => ['a', 'b'],
                'm' => ['x' => 1],
                'addr' => ['city' => 'Krakow'],
            ], ['tags' => type_array(), 'm' => type_array(), 'addr' => type_array()])]),
        );
    }

    public function test_encodes_uuid_as_string(): void
    {
        static::assertSame(
            [['f47ac10b-58cc-4372-a567-0e02b2c3d479']],
            (new ExcelEncoder())->encode([new TypedRowValues([
                'id' => new Uuid('f47ac10b-58cc-4372-a567-0e02b2c3d479'),
            ], ['id' => type_uuid()])]),
        );
    }

    public function test_encodes_null_values_as_null_cells(): void
    {
        static::assertSame(
            [[null, null]],
            (new ExcelEncoder())->encode([
                new TypedRowValues(['id' => null, 'd' => null], ['id' => type_integer(), 'd' => type_date()]),
            ]),
        );
    }
}
