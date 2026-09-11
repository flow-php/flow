<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel\Tests\Unit;

use DateInterval;
use DateTimeImmutable;
use DateTimeZone;
use DOMDocument;
use Flow\ETL\Adapter\Excel\ExcelEncoder;
use Flow\ETL\Row\TypedRowValues;
use Flow\ETL\Tests\Fixtures\Enum\BackedIntEnum;
use Flow\ETL\Tests\Fixtures\Enum\BasicEnum;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Types\Value\Uuid;
use Stringable;

use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_date;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_enum;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_json;
use function Flow\Types\DSL\type_object;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_time;
use function Flow\Types\DSL\type_time_zone;
use function Flow\Types\DSL\type_uuid;
use function Flow\Types\DSL\type_xml;

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

    public function test_headers_are_generated_when_header_is_disabled(): void
    {
        $encoder = new ExcelEncoder(withHeader: false);

        static::assertCount(1, $encoder->decode([[1, 2, 3]]));
        static::assertSame(['e00', 'e01', 'e02'], $encoder->headers());
    }

    public function test_headers_are_null_before_the_first_decode(): void
    {
        static::assertNull((new ExcelEncoder())->headers());
    }

    public function test_headers_are_stringified_and_blank_for_non_scalar_cells(): void
    {
        $encoder = new ExcelEncoder();
        $encoder->decode([[1, null, 'x']]);

        static::assertSame(['1', '', 'x'], $encoder->headers());
    }

    public function test_headers_are_the_header_row_after_decode(): void
    {
        $encoder = new ExcelEncoder();

        static::assertSame([], $encoder->decode([['id', 'name']]));
        static::assertSame(['id', 'name'], $encoder->headers());
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

    public function test_encodes_dates_and_datetimes_as_cell_values_and_intervals_with_the_time_format(): void
    {
        $date = new DateTimeImmutable('2024-08-01');
        $dateTime = new DateTimeImmutable('2024-08-01 10:30:00');

        static::assertSame(
            [[$date, $dateTime, '01:30']],
            (new ExcelEncoder(timeFormat: '%H:%I'))->encode([new TypedRowValues([
                'd' => $date,
                'dt' => $dateTime,
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

    public function test_encodes_a_pure_enum_as_its_case_name(): void
    {
        static::assertSame(
            [['three']],
            (new ExcelEncoder())->encode([
                new TypedRowValues(['e' => BasicEnum::three], ['e' => type_enum(BasicEnum::class)]),
            ]),
        );
    }

    public function test_encodes_a_stringable_as_its_string(): void
    {
        static::assertSame(
            [['Krakow']],
            (new ExcelEncoder())->encode([
                new TypedRowValues(['s' => new class implements Stringable {
                    public function __toString(): string
                    {
                        return 'Krakow';
                    }
                }], ['s' => type_object()]),
            ]),
        );
    }

    public function test_encodes_an_xml_document_as_its_serialized_root(): void
    {
        $document = new DOMDocument();
        $document->loadXML('<root><a>1</a></root>');

        static::assertSame(
            [['<root><a>1</a></root>']],
            (new ExcelEncoder())->encode([new TypedRowValues(['x' => $document], ['x' => type_xml()])]),
        );
    }

    public function test_encodes_a_value_that_does_not_match_its_declared_type_as_null(): void
    {
        static::assertSame(
            [[null, null, null, null, null, null, null]],
            (new ExcelEncoder())->encode([new TypedRowValues([
                'datetime' => 'not a datetime',
                'date' => 'not a date',
                'time' => 'not an interval',
                'enum' => 'not an enum',
                'json' => 'not a json value',
                'uuid' => 'not a uuid',
                'xml' => 'not a document',
            ], [
                'datetime' => type_datetime(),
                'date' => type_date(),
                'time' => type_time(),
                'enum' => type_enum(BasicEnum::class),
                'json' => type_json(),
                'uuid' => type_uuid(),
                'xml' => type_xml(),
            ])]),
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

    public function test_encodes_a_timezone_as_its_iana_name(): void
    {
        static::assertSame(
            [['Europe/Warsaw']],
            (new ExcelEncoder())->encode([new TypedRowValues([
                'tz' => new DateTimeZone('Europe/Warsaw'),
            ], ['tz' => type_time_zone()])]),
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
