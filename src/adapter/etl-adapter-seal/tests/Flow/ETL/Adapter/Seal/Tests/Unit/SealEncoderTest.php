<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Seal\Tests\Unit;

use DateInterval;
use DateTimeImmutable;
use DateTimeZone;
use DOMDocument;
use Flow\ETL\Adapter\Seal\SealEncoder;
use Flow\ETL\Tests\Fixtures\Enum\BackedStringEnum;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Types\Type;
use Flow\Types\Value\Json;
use Flow\Types\Value\Uuid;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\date_schema;
use function Flow\ETL\DSL\definition_from_type;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\xml_schema;
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
use function Flow\Types\DSL\type_time_zone;
use function Flow\Types\DSL\type_uuid;

final class SealEncoderTest extends FlowTestCase
{
    public static function values_provider(): Generator
    {
        yield 'string' => ['value', 'value', type_string()];
        yield 'string_nullable' => [null, null, type_string()];
        yield 'int' => [1, 1, type_integer()];
        yield 'float' => [1.1, 1.1, type_float()];
        yield 'bool' => [true, true, type_boolean()];
        yield 'uuid' => [
            new Uuid('f47ac10b-58cc-4372-a567-0e02b2c3d479'),
            'f47ac10b-58cc-4372-a567-0e02b2c3d479',
            type_uuid(),
        ];
        yield 'timezone' => [new DateTimeZone('Europe/Warsaw'), 'Europe/Warsaw', type_time_zone()];
        yield 'datetime' => [
            new DateTimeImmutable('2023-10-01 12:02:01'),
            '2023-10-01T12:02:01+00:00',
            type_datetime(),
        ];
        yield 'time' => [new DateInterval('PT1H'), 3600000000, type_time()];
        yield 'enum' => [BackedStringEnum::one, 'one', type_enum(BackedStringEnum::class)];
        yield 'json' => [new Json('{"a":1,"b":"two"}'), ['a' => 1, 'b' => 'two'], type_json()];
        yield 'list' => [['a', 'b'], ['a', 'b'], type_list(type_string())];
        yield 'list_of_datetimes' => [
            [new DateTimeImmutable('2023-10-01 12:02:01')],
            ['2023-10-01T12:02:01+00:00'],
            type_list(type_datetime()),
        ];
        yield 'map' => [['a' => 1, 'b' => 2], ['a' => 1, 'b' => 2], type_map(type_string(), type_integer())];
        yield 'structure' => [
            ['name' => 'John', 'age' => 30],
            ['name' => 'John', 'age' => 30],
            type_structure(['name' => type_string(), 'age' => type_integer()]),
        ];
        yield 'structure_with_nested_datetime' => [
            ['name' => 'John', 'created_at' => new DateTimeImmutable('2023-10-01 12:02:01')],
            ['name' => 'John', 'created_at' => '2023-10-01T12:02:01+00:00'],
            type_structure(['name' => type_string(), 'created_at' => type_datetime()]),
        ];
    }

    /**
     * @param Type<mixed> $type
     */
    #[DataProvider('values_provider')]
    public function test_encodes_values_into_documents(mixed $value, mixed $expected, Type $type): void
    {
        static::assertSame(
            $expected,
            (new SealEncoder())->encode(array_to_rows([[
                'field' => $value,
            ]], schema(definition_from_type('field', $type, nullable: $value === null))))[0]['field'],
        );
    }

    public function test_encodes_xml_document_to_string(): void
    {
        $doc = new DOMDocument();
        $doc->loadXML('<root><a>1</a></root>');

        static::assertSame(
            '<root><a>1</a></root>',
            (new SealEncoder())->encode(array_to_rows([['xml' => $doc]], schema(xml_schema('xml'))))[0]['xml'],
        );
    }

    public function test_renders_date_with_the_date_format(): void
    {
        static::assertSame(
            '2023-10-01',
            (new SealEncoder())->encode(array_to_rows([[
                'date' => new DateTimeImmutable('2023-10-01 00:00:00'),
            ]], schema(date_schema('date'))))[0]['date'],
        );
    }

    public function test_produces_documents_keyed_by_column_name(): void
    {
        static::assertSame(
            [['id' => 1, 'name' => 'Alice'], ['id' => 2, 'name' => 'Bob']],
            (new SealEncoder())->encode(array_to_rows(
                [['id' => 1, 'name' => 'Alice'], ['id' => 2, 'name' => 'Bob']],
                schema(int_schema('id'), str_schema('name')),
            )),
        );
    }
}
