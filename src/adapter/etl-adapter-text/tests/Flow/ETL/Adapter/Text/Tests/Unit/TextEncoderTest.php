<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Text\Tests\Unit;

use Flow\ETL\Adapter\Text\TextEncoder;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Schema\Definition;
use Flow\ETL\Tests\FlowTestCase;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\map_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\structure_schema;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;

final class TextEncoderTest extends FlowTestCase
{
    /**
     * @return Generator<string, array{Definition<mixed>, mixed}>
     */
    public static function nested_columns(): Generator
    {
        yield 'list' => [list_schema('a', type_list(type_integer())), [1]];
        yield 'map' => [map_schema('a', type_map(type_string(), type_integer())), ['x' => 1]];
        yield 'structure' => [structure_schema('a', type_structure(['x' => type_integer()])), ['x' => 1]];
    }

    public function test_encode_writes_every_value_on_its_own_line_and_a_null_as_an_empty_line(): void
    {
        static::assertSame(
            "first\n\nsecond\n",
            (new TextEncoder("\n"))->encode(array_to_rows([
                ['text' => 'first'],
                ['text' => null],
                ['text' => 'second'],
            ], schema(str_schema('text', nullable: true)))),
        );
    }

    public function test_encode_writes_the_text_of_a_value_that_is_no_string(): void
    {
        static::assertSame(
            "0.30000000000000004\n1.0\n",
            (new TextEncoder("\n"))->encode(array_to_rows([
                ['price' => 0.1 + 0.2],
                ['price' => 1.0],
            ], schema(float_schema('price')))),
        );
    }

    /**
     * @param Definition<mixed> $definition
     */
    #[DataProvider('nested_columns')]
    public function test_encode_refuses_a_nested_column(Definition $definition, mixed $value): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Text data loader supports only scalar values, got array');

        (new TextEncoder())->encode(array_to_rows([['a' => $value]], schema($definition)));
    }

    public function test_encode_ends_every_line_with_the_configured_separator(): void
    {
        static::assertSame(
            "first\r\nsecond\r\n",
            (new TextEncoder("\r\n"))->encode(array_to_rows([
                ['text' => 'first'],
                ['text' => 'second'],
            ], schema(str_schema('text')))),
        );
    }

    public function test_encode_throws_when_a_row_has_more_than_one_column(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Text data loader writes at most one column, the batch has 3 columns.');

        (new TextEncoder())->encode(array_to_rows(
            [['a' => 1, 'b' => 2, 'c' => 3], ['a' => 4, 'b' => 5, 'c' => 6]],
            schema(int_schema('a'), int_schema('b'), int_schema('c')),
        ));
    }

    public function test_encode_of_an_empty_batch_with_more_than_one_column_returns_no_lines(): void
    {
        static::assertSame(
            '',
            (new TextEncoder())->encode(array_to_rows([], schema(int_schema('a'), int_schema('b')))),
        );
    }
}
