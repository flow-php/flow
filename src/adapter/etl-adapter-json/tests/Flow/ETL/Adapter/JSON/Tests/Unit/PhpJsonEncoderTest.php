<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\Tests\Unit;

use DateInterval;
use DateTimeImmutable;
use DateTimeZone;
use Flow\ETL\Adapter\JSON\PhpJsonEncoder;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Schema\Definition;
use Flow\ETL\Tests\Fixtures\Enum\BackedStringEnum;
use Flow\ETL\Tests\Fixtures\Enum\BasicEnum;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Types\Value\Uuid;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\RequiresPhp;
use PHPUnit\Framework\Attributes\TestWith;

use function array_map;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\date_schema;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\enum_schema;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\html_element_schema;
use function Flow\ETL\DSL\html_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\json_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\map_schema;
use function Flow\ETL\DSL\null_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\structure_schema;
use function Flow\ETL\DSL\time_schema;
use function Flow\ETL\DSL\time_zone_schema;
use function Flow\ETL\DSL\uuid_schema;
use function Flow\ETL\DSL\xml_element_schema;
use function Flow\ETL\DSL\xml_schema;
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
use function Flow\Types\DSL\type_uuid;
use function ini_get;
use function ini_set;

use const JSON_PRESERVE_ZERO_FRACTION;
use const JSON_PRETTY_PRINT;
use const JSON_THROW_ON_ERROR;
use const JSON_UNESCAPED_SLASHES;
use const JSON_UNESCAPED_UNICODE;

final class PhpJsonEncoderTest extends FlowTestCase
{
    /**
     * @return Generator<string, array{Definition<mixed>, list<mixed>, string}>
     */
    public static function one_column_batches(): Generator
    {
        yield "int_schema('a', nullable: true)" => [int_schema('a', nullable: true), [1, null], '{"a":1}|{"a":null}'];
        yield "float_schema('a', nullable: true)" => [
            float_schema('a', nullable: true),
            [9.5, 1.0, 0.1 + 0.2, 1.0e25, null],
            '{"a":9.5}|{"a":1}|{"a":0.30000000000000004}|{"a":1.0e+25}|{"a":null}',
        ];
        yield "bool_schema('a', nullable: true)" => [
            bool_schema('a', nullable: true),
            [true, false, null],
            '{"a":true}|{"a":false}|{"a":null}',
        ];
        yield "str_schema('a', nullable: true)" => [
            str_schema('a', nullable: true),
            ['Alice', 'a/b "ż"', null],
            '{"a":"Alice"}|{"a":"a\/b \"\u017c\""}|{"a":null}',
        ];
        yield "datetime_schema('a')" => [
            datetime_schema('a', nullable: true),
            [new DateTimeImmutable('2023-10-01 12:02:01 UTC'), null],
            '{"a":"2023-10-01T12:02:01+00:00"}|{"a":null}',
        ];
        yield "date_schema('a')" => [
            date_schema('a'),
            [new DateTimeImmutable('2023-10-01 12:02:01 UTC')],
            '{"a":"2023-10-01"}',
        ];
        yield "time_schema('a')" => [time_schema('a'), [new DateInterval('PT1H')], '{"a":3600000000}'];
        yield "uuid_schema('a')" => [
            uuid_schema('a'),
            [new Uuid('f47ac10b-58cc-4372-a567-0e02b2c3d479')],
            '{"a":"f47ac10b-58cc-4372-a567-0e02b2c3d479"}',
        ];
        yield "enum_schema('a', BackedStringEnum::class)" => [
            enum_schema('a', BackedStringEnum::class),
            [BackedStringEnum::one],
            '{"a":"one"}',
        ];
        yield "enum_schema('a', BasicEnum::class)" => [
            enum_schema('a', BasicEnum::class),
            [BasicEnum::two],
            '{"a":"two"}',
        ];
        yield "time_zone_schema('a')" => [
            time_zone_schema('a'),
            [new DateTimeZone('Europe/Warsaw')],
            '{"a":"Europe\/Warsaw"}',
        ];
        yield "json_schema('a') keeps the stored shapes" => [
            json_schema('a', nullable: true),
            ['{"a": 1, "b": 2}', '{}', '[]', '{"a":{}}', '[1, {"0": "x"}]', null],
            '{"a":{"a":1,"b":2}}|{"a":{}}|{"a":[]}|{"a":{"a":{}}}|{"a":[1,{"0":"x"}]}|{"a":null}',
        ];
        yield "xml_schema('a')" => [xml_schema('a'), ['<root><a>1</a></root>'], '{"a":"<root><a>1<\/a><\/root>"}'];
        yield "xml_element_schema('a')" => [
            xml_element_schema('a'),
            ['<a b="1"><c/></a>'],
            '{"a":"<a b=\"1\"><c><\/c><\/a>"}',
        ];
        yield "null_schema('a')" => [null_schema('a'), [null], '{"a":null}'];
        yield 'list<integer>, empty list' => [
            list_schema('a', type_list(type_integer()), nullable: true),
            [[1, 2, 3], [], null],
            '{"a":[1,2,3]}|{"a":[]}|{"a":null}',
        ];
        yield 'list<structure>' => [
            list_schema('a', type_list(type_structure(['t' => type_string()]))),
            [[['t' => 'a'], ['t' => 'b']]],
            '{"a":[{"t":"a"},{"t":"b"}]}',
        ];
        yield 'B2 list<date> and structure{d: date}' => [
            structure_schema('a', type_structure(['d' => type_date(), 'l' => type_list(type_date())])),
            [['d' => new DateTimeImmutable('2026-01-02'), 'l' => [new DateTimeImmutable('2026-01-03')]]],
            '{"a":{"d":"2026-01-02","l":["2026-01-03"]}}',
        ];
        yield 'B8 map<string, string>' => [
            map_schema('a', type_map(type_string(), type_string())),
            [['en' => 'red', 'pl' => 'czerwony']],
            '{"a":{"en":"red","pl":"czerwony"}}',
        ];
        yield 'B8 map<int, string> keyed 0, 1' => [
            map_schema('a', type_map(type_integer(), type_string())),
            [[0 => 'a', 1 => 'b']],
            '{"a":{"0":"a","1":"b"}}',
        ];
        yield 'B8 map<int, string> keyed 5, 7' => [
            map_schema('a', type_map(type_integer(), type_string())),
            [[5 => 'a', 7 => 'b']],
            '{"a":{"5":"a","7":"b"}}',
        ];
        yield 'B8 empty map' => [map_schema('a', type_map(type_string(), type_integer())), [[]], '{"a":{}}'];
        yield 'structure of datetime, uuid, json and enum leaves' => [
            structure_schema('a', type_structure([
                'at' => type_datetime(),
                'id' => type_uuid(),
                'payload' => type_json(),
                'level' => type_enum(BackedStringEnum::class),
            ])),
            [[
                'at' => new DateTimeImmutable('2023-10-01 12:02:01 UTC'),
                'id' => new Uuid('f47ac10b-58cc-4372-a567-0e02b2c3d479'),
                'payload' => '{"a":1}',
                'level' => BackedStringEnum::one,
            ]],
            '{"a":{"at":"2023-10-01T12:02:01+00:00","id":"f47ac10b-58cc-4372-a567-0e02b2c3d479","payload":{"a":1},"level":"one"}}',
        ];
        yield 'structure of time, float and boolean leaves' => [
            structure_schema('a', type_structure([
                'took' => type_time(),
                'score' => type_float(),
                'active' => type_boolean(),
            ])),
            [['took' => new DateInterval('PT1H'), 'score' => 9.5, 'active' => true]],
            '{"a":{"took":3600000000,"score":9.5,"active":true}}',
        ];
    }

    /**
     * @param Definition<mixed> $definition
     * @param list<mixed> $values
     */
    #[DataProvider('one_column_batches')]
    public function test_encode_renders_a_column_by_its_type(
        Definition $definition,
        array $values,
        string $expected,
    ): void {
        static::assertSame($expected, (new PhpJsonEncoder())->encode(
            array_to_rows(array_map(static fn(mixed $value): array => ['a' => $value], $values), schema($definition)),
            '|',
        ));
    }

    #[RequiresPhp('>= 8.4.0')]
    public function test_encode_renders_html_as_its_markup(): void
    {
        static::assertSame('{"doc":"<!DOCTYPE html><html><head><\/head><body><p>a<\/p><\/body><\/html>","el":"<p>a<\/p>"}', (new PhpJsonEncoder())->encode(
            array_to_rows(
                [['doc' => '<!DOCTYPE html><html><head></head><body><p>a</p></body></html>', 'el' => '<p>a</p>']],
                schema(html_schema('doc'), html_element_schema('el')),
            ),
            "\n",
        ));
    }

    public function test_encode_writes_one_object_per_row_in_schema_order(): void
    {
        static::assertSame('{"id":1,"name":"Alice","active":true,"score":9.5,"missing":null}'
        . "\n"
        . '{"id":2,"name":"Bob","active":false,"score":1,"missing":"x"}', (new PhpJsonEncoder())->encode(
            array_to_rows(
                [
                    ['name' => 'Alice', 'id' => 1, 'active' => true, 'score' => 9.5, 'missing' => null],
                    ['id' => 2, 'name' => 'Bob', 'active' => false, 'score' => 1.0, 'missing' => 'x'],
                ],
                schema(
                    int_schema('id'),
                    str_schema('name'),
                    bool_schema('active'),
                    float_schema('score'),
                    str_schema('missing', nullable: true),
                ),
            ),
            "\n",
        ));
    }

    public function test_a_row_of_columns_named_0_and_1_is_an_object(): void
    {
        static::assertSame('{"0":"a","1":"b"}', (new PhpJsonEncoder())->encode(
            array_to_rows([['0' => 'a', '1' => 'b']], schema(str_schema('0'), str_schema('1'))),
            ',',
        ));
    }

    public function test_encode_of_an_empty_batch_is_empty(): void
    {
        static::assertSame('', (new PhpJsonEncoder())->encode(array_to_rows([], schema(int_schema('a'))), ','));
    }

    #[TestWith([JSON_THROW_ON_ERROR, '{"a":"a\/\u017c","f":1}'])]
    #[TestWith([JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES, '{"a":"a/\u017c","f":1}'])]
    #[TestWith([JSON_THROW_ON_ERROR | JSON_UNESCAPED_UNICODE, '{"a":"a\/ż","f":1}'])]
    #[TestWith([JSON_THROW_ON_ERROR | JSON_PRESERVE_ZERO_FRACTION, '{"a":"a\/\u017c","f":1.0}'])]
    #[TestWith([JSON_PRETTY_PRINT, "{\n    \"a\": \"a\\/\\u017c\",\n    \"f\": 1\n}"])]
    public function test_encode_applies_the_flags(int $flags, string $expected): void
    {
        static::assertSame($expected, (new PhpJsonEncoder($flags))->encode(
            array_to_rows([['a' => 'a/ż', 'f' => 1.0]], schema(str_schema('a'), float_schema('f'))),
            ',',
        ));
    }

    public function test_encode_uses_the_configured_formats_at_every_depth(): void
    {
        static::assertSame('{"at":"01\/10\/2023 12:02","on":"2023\/10\/01","l":["01\/10\/2023 12:02"]}', (new PhpJsonEncoder(
            JSON_THROW_ON_ERROR,
            'd/m/Y H:i',
            'Y/m/d',
        ))->encode(
            array_to_rows(
                [[
                    'at' => new DateTimeImmutable('2023-10-01 12:02:01 UTC'),
                    'on' => new DateTimeImmutable('2023-10-01'),
                    'l' => [new DateTimeImmutable('2023-10-01 12:02:01 UTC')],
                ]],
                schema(datetime_schema('at'), date_schema('on'), list_schema('l', type_list(type_datetime()))),
            ),
            ',',
        ));
    }

    public function test_float_text_does_not_depend_on_serialize_precision(): void
    {
        $previous = (string) ini_get('serialize_precision');
        ini_set('serialize_precision', '17');

        try {
            $encoder = new PhpJsonEncoder();
            $rows = array_to_rows(
                [['f' => 0.1, 'l' => [1.0e25]]],
                schema(float_schema('f'), list_schema('l', type_list(type_float()))),
            );

            static::assertSame('{"f":0.1,"l":[1.0e+25]}', $encoder->encode($rows, ','));
            static::assertSame(['0.1'], $encoder->fragments($rows->schema()->get('f')->type(), $rows->column('f')));
            static::assertSame('17', ini_get('serialize_precision'));
        } finally {
            ini_set('serialize_precision', $previous);
        }
    }

    #[TestWith([JSON_THROW_ON_ERROR])]
    #[TestWith([0])]
    public function test_a_not_finite_float_is_refused(int $flags): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Failed to encode JSON: Inf and NaN cannot be JSON encoded');

        (new PhpJsonEncoder($flags))->encode(array_to_rows([['f' => NAN]], schema(float_schema('f'))), ',');
    }

    public function test_malformed_utf8_is_refused(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage(
            'Failed to encode JSON: Malformed UTF-8 characters, possibly incorrectly encoded',
        );

        (new PhpJsonEncoder())->encode(array_to_rows([['s' => "\xff"]], schema(str_schema('s'))), ',');
    }

    public function test_fragments_are_the_json_text_of_every_cell(): void
    {
        $rows = array_to_rows(
            [['j' => '{"a": {}}', 'm' => [], 's' => 'a/b'], ['j' => null, 'm' => [1 => 2.0], 's' => null]],
            schema(
                json_schema('j', nullable: true),
                map_schema('m', type_map(type_integer(), type_float())),
                str_schema('s', nullable: true),
            ),
        );
        $encoder = new PhpJsonEncoder(JSON_THROW_ON_ERROR | JSON_PRESERVE_ZERO_FRACTION | JSON_UNESCAPED_SLASHES);
        $fragments = [];

        foreach ($rows->schema()->definitions() as $definition) {
            $fragments[] = $encoder->fragments($definition->type(), $rows->column($definition->entry()->name()));
        }

        static::assertSame([['{"a":{}}', 'null'], ['{}', '{"1":2.0}'], ['"a/b"', 'null']], $fragments);
    }
}
