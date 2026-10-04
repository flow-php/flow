<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\Tests\Unit;

use DateTimeImmutable;
use Flow\ETL\Adapter\CSV\CSVWriteOptions;
use Flow\ETL\Adapter\CSV\PhpCSVEncoder;
use Flow\ETL\Tests\FlowTestCase;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\TestWith;

use function array_fill;
use function array_map;
use function chr;
use function fclose;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\date_schema;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\json_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_string;
use function fopen;
use function fputcsv;
use function implode;
use function mt_rand;
use function mt_srand;
use function stream_get_contents;

final class PhpCSVEncoderTest extends FlowTestCase
{
    /**
     * @return Generator<string, array{?string, string}>
     */
    public static function quoting(): Generator
    {
        yield 'separator' => ['a,b', "\"a,b\"\n"];
        yield 'enclosure' => ['say "hi"', "\"say \"\"hi\"\"\"\n"];
        yield 'line feed' => ["two\nlines", "\"two\nlines\"\n"];
        yield 'carriage return' => ["two\rlines", "\"two\rlines\"\n"];
        yield 'escape before an enclosure' => ['back\slash"q', "\"back\\slash\"\"q\"\n"];
        yield 'an enclosure right after the escape character' => ['a\"b', "\"a\\\"b\"\n"];
        yield 'space' => [' lead', "\" lead\"\n"];
        yield 'tab' => ["tab\t", "\"tab\t\"\n"];
        yield 'empty string' => ['', "\n"];
        yield 'null' => [null, "\n"];
        yield 'plain' => ['plain', "plain\n"];
    }

    #[DataProvider('quoting')]
    public function test_encode_encloses_a_field_as_fputcsv_does(?string $value, string $expected): void
    {
        static::assertSame(
            $expected,
            (new PhpCSVEncoder(new CSVWriteOptions(newLineSeparator: "\n")))->encode(array_to_rows([[
                'a' => $value,
            ]], schema(str_schema('a', nullable: true)))),
        );
    }

    public function test_encode_with_a_custom_separator_and_enclosure(): void
    {
        static::assertSame(
            "'a;b';1;'it''s'\r\nplain;2;\"q\"\r\n",
            (new PhpCSVEncoder(new CSVWriteOptions(
                separator: ';',
                enclosure: "'",
                newLineSeparator: "\r\n",
            )))->encode(array_to_rows(
                [['a' => 'a;b', 'b' => 1, 'c' => "it's"], ['a' => 'plain', 'b' => 2, 'c' => '"q"']],
                schema(str_schema('a'), int_schema('b'), str_schema('c')),
            )),
        );
    }

    public function test_encode_without_an_escape_character_doubles_every_enclosure(): void
    {
        static::assertSame(
            "\"a\\\"\"b\"\nback\\slash\n",
            (new PhpCSVEncoder(new CSVWriteOptions(escape: '', newLineSeparator: "\n")))->encode(array_to_rows([
                ['a' => 'a\"b'],
                ['a' => 'back\slash'],
            ], schema(str_schema('a')))),
        );
    }

    public function test_encode_encloses_a_datetime_whose_format_holds_a_space(): void
    {
        static::assertSame(
            "\"2023-10-01 12:02:01\",\"01 Oct 2023\"\n",
            (new PhpCSVEncoder(new CSVWriteOptions(
                newLineSeparator: "\n",
                dateTimeFormat: 'Y-m-d H:i:s',
                dateFormat: 'd M Y',
            )))->encode(array_to_rows(
                [[
                    'at' => new DateTimeImmutable('2023-10-01 12:02:01 UTC'),
                    'on' => new DateTimeImmutable('2023-10-01'),
                ]],
                schema(datetime_schema('at'), date_schema('on')),
            )),
        );
    }

    /**
     * A separator that the text of a number, a boolean or a uuid can hold keeps those columns enclosed.
     */
    #[TestWith(['.', "\"1.5\".-2.true.\"1.0e+25\"\n"])]
    #[TestWith(['-', "1.5-\"-2\"-true-1.0e+25\n"])]
    #[TestWith(['e', "1.5e-2e\"true\"e\"1.0e+25\"\n"])]
    public function test_encode_encloses_numbers_and_booleans_that_hold_the_separator(
        string $separator,
        string $expected,
    ): void {
        static::assertSame(
            $expected,
            (new PhpCSVEncoder(new CSVWriteOptions(
                separator: $separator,
                newLineSeparator: "\n",
            )))->encode(array_to_rows(
                [['f' => 1.5, 'i' => -2, 'b' => true, 'g' => 1.0e25]],
                schema(float_schema('f'), int_schema('i'), bool_schema('b'), float_schema('g')),
            )),
        );
    }

    public function test_encode_renders_eleven_columns_in_schema_order(): void
    {
        $definitions = [];
        $row = [];

        for ($i = 0; $i < 11; $i++) {
            $definitions[] = int_schema('c' . $i);
            $row['c' . $i] = $i;
        }

        static::assertSame(
            "0,1,2,3,4,5,6,7,8,9,10\n0,1,2,3,4,5,6,7,8,9,10\n",
            (new PhpCSVEncoder(new CSVWriteOptions(newLineSeparator: "\n")))->encode(array_to_rows(
                array_fill(0, 2, $row),
                schema(...$definitions),
            )),
        );
    }

    public function test_encode_writes_random_strings_as_fputcsv_does(): void
    {
        mt_srand(8);
        $alphabet = ['a', 'b', ',', '"', '\\', "\n", "\r", "\t", ' ', ';', 'ż', ''];
        $rows = [];

        for ($i = 0; $i < 200; $i++) {
            $row = [];

            foreach (['a', 'b', 'c'] as $name) {
                $cell = '';

                for ($j = mt_rand(0, 6); $j > 0; $j--) {
                    $cell .= $alphabet[mt_rand(0, 11)];
                }

                $row[$name] = $cell;
            }

            $rows[] = $row;
        }

        $reference = fopen('php://memory', 'rb+');
        static::assertIsResource($reference);

        foreach ($rows as $row) {
            fputcsv($reference, $row, ',', '"', '\\', "\n");
        }

        $expected = stream_get_contents($reference, offset: 0);
        fclose($reference);

        static::assertSame(
            $expected,
            (new PhpCSVEncoder(new CSVWriteOptions(newLineSeparator: "\n")))->encode(array_to_rows($rows, schema(
                str_schema('a'),
                str_schema('b'),
                str_schema('c'),
            ))),
        );
    }

    public function test_encode_of_an_empty_batch_is_empty(): void
    {
        static::assertSame(
            '',
            (new PhpCSVEncoder(new CSVWriteOptions()))->encode(array_to_rows([], schema(int_schema('a')))),
        );
    }

    public function test_encode_writes_a_null_as_an_empty_field_and_encloses_a_nested_cell(): void
    {
        static::assertSame(
            "1,true,\"[\"\"a b\"\",\"\"c\"\"]\",2023-10-01T12:02:01+00:00\n,,,\n",
            (new PhpCSVEncoder(new CSVWriteOptions(newLineSeparator: "\n")))->encode(array_to_rows(
                [
                    [
                        'id' => 1,
                        'active' => true,
                        'tags' => ['a b', 'c'],
                        'at' => new DateTimeImmutable('2023-10-01 12:02:01 UTC'),
                    ],
                    ['id' => null, 'active' => null, 'tags' => null, 'at' => null],
                ],
                schema(
                    int_schema('id', nullable: true),
                    bool_schema('active', nullable: true),
                    list_schema('tags', type_list(type_string()), nullable: true),
                    datetime_schema('at', nullable: true),
                ),
            )),
        );
    }

    public function test_encode_header(): void
    {
        static::assertSame(
            "id;'first name';'it''s'|",
            (new PhpCSVEncoder(new CSVWriteOptions(
                separator: ';',
                enclosure: "'",
                newLineSeparator: '|',
            )))->encodeHeader([
                'id',
                'first name',
                "it's",
            ]),
        );
    }

    public function test_cells_are_the_fields_before_quoting(): void
    {
        $rows = array_to_rows(
            [
                ['s' => 'a,b', 'j' => '{"a": 1}', 'l' => ['x y'], 'i' => 7, 'b' => false],
                ['s' => null, 'j' => null, 'l' => null, 'i' => null, 'b' => null],
            ],
            schema(
                str_schema('s', nullable: true),
                json_schema('j', nullable: true),
                list_schema('l', type_list(type_string()), nullable: true),
                int_schema('i', nullable: true),
                bool_schema('b', nullable: true),
            ),
        );
        $encoder = new PhpCSVEncoder(new CSVWriteOptions());
        $cells = [];

        foreach ($rows->schema()->definitions() as $definition) {
            $cells[] = $encoder->cells($definition->type(), $rows->column($definition->entry()->name()));
        }

        static::assertSame(
            [['a,b', null], ['{"a": 1}', null], ['["x y"]', null], ['7', null], ['false', null]],
            $cells,
        );
    }

    public function test_encode_keeps_every_byte_of_a_binary_string(): void
    {
        $bytes = implode('', array_map(chr(...), [0, 1, 127, 128, 255]));

        static::assertSame(
            $bytes . "\n",
            (new PhpCSVEncoder(new CSVWriteOptions(newLineSeparator: "\n")))->encode(array_to_rows([[
                'a' => $bytes,
            ]], schema(str_schema('a')))),
        );
    }
}
