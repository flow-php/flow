<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel\Tests\Unit;

use DateInterval;
use DateTimeImmutable;
use DateTimeZone;
use DOMDocument;
use Flow\ETL\Adapter\Excel\ExcelEncoder;
use Flow\ETL\Tests\Fixtures\Enum\BackedIntEnum;
use Flow\ETL\Tests\Fixtures\Enum\BasicEnum;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Types\Value\Uuid;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\date_schema;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\enum_schema;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\map_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\structure_schema;
use function Flow\ETL\DSL\time_schema;
use function Flow\ETL\DSL\time_zone_schema;
use function Flow\ETL\DSL\uuid_schema;
use function Flow\ETL\DSL\xml_schema;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;

final class ExcelEncoderTest extends FlowTestCase
{
    public function test_encodes_scalars_as_a_flat_cell_list_in_value_order(): void
    {
        static::assertSame(
            [[1, 'Norbert', 1.5, true]],
            (new ExcelEncoder())->encode(array_to_rows(
                [['id' => 1, 'name' => 'Norbert', 'p' => 1.5, 'a' => true]],
                schema(int_schema('id'), str_schema('name'), float_schema('p'), bool_schema('a')),
            )),
        );
    }

    public function test_encodes_dates_and_datetimes_as_cell_values_and_intervals_with_the_time_format(): void
    {
        $date = new DateTimeImmutable('2024-08-01');
        $dateTime = new DateTimeImmutable('2024-08-01 10:30:00');

        static::assertEquals(
            [[$date, $dateTime, '01:30']],
            (new ExcelEncoder(timeFormat: '%H:%I'))->encode(array_to_rows(
                [['d' => $date, 'dt' => $dateTime, 't' => new DateInterval('PT1H30M')]],
                schema(date_schema('d'), datetime_schema('dt'), time_schema('t')),
            )),
        );
    }

    public function test_encodes_time_with_the_default_time_format(): void
    {
        static::assertSame(
            [['14:30:45']],
            (new ExcelEncoder())->encode(array_to_rows([[
                't' => new DateInterval('PT14H30M45S'),
            ]], schema(time_schema('t')))),
        );
    }

    public function test_encodes_backed_enum_as_its_backing_value(): void
    {
        static::assertSame(
            [['1']],
            (new ExcelEncoder())->encode(array_to_rows([[
                'e' => BackedIntEnum::one,
            ]], schema(enum_schema('e', BackedIntEnum::class)))),
        );
    }

    public function test_encodes_a_pure_enum_as_its_case_name(): void
    {
        static::assertSame(
            [['three']],
            (new ExcelEncoder())->encode(array_to_rows([[
                'e' => BasicEnum::three,
            ]], schema(enum_schema('e', BasicEnum::class)))),
        );
    }

    public function test_encodes_an_xml_document_as_its_serialized_root(): void
    {
        $document = new DOMDocument();
        $document->loadXML('<root><a>1</a></root>');

        static::assertSame(
            [['<root><a>1</a></root>']],
            (new ExcelEncoder())->encode(array_to_rows([['x' => $document]], schema(xml_schema('x')))),
        );
    }

    public function test_encodes_containers_as_json(): void
    {
        static::assertSame(
            [['["a","b"]', '{"x":1}', '{"city":"Krakow"}']],
            (new ExcelEncoder())->encode(array_to_rows(
                [['tags' => ['a', 'b'], 'm' => ['x' => 1], 'addr' => ['city' => 'Krakow']]],
                schema(
                    list_schema('tags', type_list(type_string())),
                    map_schema('m', type_map(type_string(), type_integer())),
                    structure_schema('addr', type_structure(['city' => type_string()])),
                ),
            )),
        );
    }

    public function test_encodes_a_timezone_as_its_iana_name(): void
    {
        static::assertSame(
            [['Europe/Warsaw']],
            (new ExcelEncoder())->encode(array_to_rows([[
                'tz' => new DateTimeZone('Europe/Warsaw'),
            ]], schema(time_zone_schema('tz')))),
        );
    }

    public function test_encodes_uuid_as_string(): void
    {
        static::assertSame(
            [['f47ac10b-58cc-4372-a567-0e02b2c3d479']],
            (new ExcelEncoder())->encode(array_to_rows([[
                'id' => new Uuid('f47ac10b-58cc-4372-a567-0e02b2c3d479'),
            ]], schema(uuid_schema('id')))),
        );
    }

    public function test_encodes_null_values_as_null_cells(): void
    {
        static::assertSame(
            [[null, null]],
            (new ExcelEncoder())->encode(array_to_rows(
                [['id' => null, 'd' => null]],
                schema(int_schema('id', nullable: true), date_schema('d', nullable: true)),
            )),
        );
    }
}
