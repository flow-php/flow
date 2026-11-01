<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\IO\Inference;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\IO\Inference\ColumnTypes;
use Flow\ETL\IO\Inference\InferredTypes;
use Flow\ETL\Tests\Double\RecordingSources;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\ColumnTypesMother;
use Flow\ETL\Values\Schema;
use Flow\Types\Type;
use Flow\Types\Type\Native\String\StringTypeNarrower;
use PHPUnit\Framework\Attributes\DataProvider;

use function array_keys;
use function Flow\ETL\DSL\definition_from_type;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_json;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;

final class ColumnTypesTest extends FlowTestCase
{
    /**
     * Every ladder edge case the epic's c1 probe carries, plus the reorder's own fixtures, so the goal -
     * "a type it declares is one every row will fit" - is pinned for the very row that produced the type.
     *
     * @return array<string, array{string}>
     */
    public static function ladderCells(): array
    {
        return [
            'cell 1' => ['1'],
            'cell 0' => ['0'],
            'cell 123' => ['123'],
            'cell 01234' => ['01234'],
            'cell -0' => ['-0'],
            'cell +5' => ['+5'],
            'cell 1e5' => ['1e5'],
            'cell 1E5' => ['1E5'],
            'cell 1.0' => ['1.0'],
            'cell 1,234' => ['1,234'],
            'cell 1_000' => ['1_000'],
            'cell $1.23' => ['$1.23'],
            'cell 12.50%' => ['12.50%'],
            'cell TRUE' => ['TRUE'],
            'cell True' => ['True'],
            'cell t' => ['t'],
            'cell f' => ['f'],
            'cell yes' => ['yes'],
            'cell no' => ['no'],
            'cell 1.5e-3' => ['1.5e-3'],
            'cell NaN' => ['NaN'],
            'cell inf' => ['inf'],
            'cell INF' => ['INF'],
            'cell -inf' => ['-inf'],
            'cell 0x1A' => ['0x1A'],
            'cell 1970' => ['1970'],
            'cell 20240101' => ['20240101'],
            'cell 2024-13-01' => ['2024-13-01'],
            'cell 12/31/2024' => ['12/31/2024'],
            'cell 31/12/2024' => ['31/12/2024'],
            'cell March 5' => ['March 5'],
            'cell tomorrow' => ['tomorrow'],
            'cell now' => ['now'],
            'cell 12:30:00' => ['12:30:00'],
            'cell 12:30' => ['12:30'],
            'cell 2024-01-01 10:00' => ['2024-01-01 10:00'],
            'cell 2024-01-01T10:00:00+02:00' => ['2024-01-01T10:00:00+02:00'],
            'cell 1 hour' => ['1 hour'],
            'cell []' => ['[]'],
            'cell {}' => ['{}'],
            'cell [1,2]' => ['[1,2]'],
            'cell "quoted"' => ['"quoted"'],
            'cell null' => ['null'],
            'cell NULL' => ['NULL'],
            'cell nil' => ['nil'],
            'cell N/A' => ['N/A'],
            'cell NA' => ['NA'],
            'cell -' => ['-'],
            'cell   ' => ['  '],
            'cell \\N' => ['\\N'],
            'cell 9223372036854775808' => ['9223372036854775808'],
            'cell 1.7976931348623157e309' => ['1.7976931348623157e309'],
            'cell 0.1' => ['0.1'],
            'cell .5' => ['.5'],
            'cell 5.' => ['5.'],
            'cell 00' => ['00'],
            'cell 1 234' => ['1 234'],
            'cell 19991231' => ['19991231'],
            'cell 1012024' => ['1012024'],
            'cell true' => ['true'],
            'cell false' => ['false'],
            'cell {"a":1}' => ['{"a":1}'],
            'cell f47ac10b-58cc-4372-a567-0e02b2c3d479' => ['f47ac10b-58cc-4372-a567-0e02b2c3d479'],
            'cell Europe/Warsaw' => ['Europe/Warsaw'],
            'cell +02:00' => ['+02:00'],
            'cell 2024-01-01' => ['2024-01-01'],
            'cell  12 ' => [' 12 '],
            'cell <a><b>1</b></a>' => ['<a><b>1</b></a>'],
            'the empty cell' => [''],
            'cell -7' => ['-7'],
            'cell 1.5' => ['1.5'],
        ];
    }

    /**
     * @return array<string, array{list<mixed>, Type<mixed>}>
     */
    public static function typedValueFolds(): array
    {
        return [
            'int then float' => [[1, 1.5], type_float()],
            'empty array then object' => [[[], ['a' => 1]], type_json()],
            'empty array then int list' => [[[], [1]], type_list(type_optional(type_integer()))],
            'only an empty array' => [[[]], type_json()],
            'structure with a null element' => [
                [['a' => null], ['a' => 1]],
                type_structure(['a' => type_optional(type_integer())]),
            ],
        ];
    }

    /**
     * @return array<string, array{list<string>, Type<mixed>}>
     */
    public static function widenedColumns(): array
    {
        return [
            'integer then float' => [['1', '1.5'], type_float()],
            'integer then text' => [['1', 'x'], type_string()],
            'date then datetime' => [['2024-01-01', '2024-01-01 10:00'], type_datetime()],
            'both booleans' => [['true', 'false'], type_boolean()],
            'free text then datetime' => [['free text', '2024-01-01 10:00:00'], type_string()],
            'free text then integer' => [['free text', '42'], type_string()],
            'free text then json' => [['free text', '{"a":1}'], type_string()],
            'free text then uuid' => [['free text', 'f47ac10b-58cc-4372-a567-0e02b2c3d479'], type_string()],
        ];
    }

    public function test_a_header_only_source_yields_every_name_as_nullable_string(): void
    {
        static::assertEquals(
            new Schema(
                definition_from_type('id', type_string(), nullable: true),
                definition_from_type('name', type_string(), nullable: true),
            ),
            ColumnTypesMother::fromStrings(['id', 'name'])->folded()->schema(ColumnTypesMother::floor()),
        );
    }

    public function test_a_name_met_only_in_a_row_is_appended_after_the_header_names(): void
    {
        $columns = ColumnTypesMother::fromStrings(['a', 'b']);
        $columns->observe(['c' => '1']);

        static::assertSame(
            ['a', 'b', 'c'],
            array_keys($columns->folded()->schema(ColumnTypesMother::floor())->definitions()),
        );
    }

    public function test_a_null_first_row_then_free_text_yields_optional_string(): void
    {
        $columns = ColumnTypesMother::fromStrings();
        $columns->observe(['c' => null]);
        $columns->observe(['c' => 'free text']);
        $columns->observe(['c' => '42']);

        static::assertEquals(
            new Schema(definition_from_type('c', type_optional(type_string()), nullable: true)),
            $columns->folded()->schema(ColumnTypesMother::floor()),
        );
    }

    public function test_a_numeric_key_becomes_a_string_column_name(): void
    {
        $columns = ColumnTypesMother::fromStrings();
        // PHP re-keys a numeric column name to int, so the fold has to survive an int key
        $columns->observe(['1' => 'x']);

        static::assertSame('1', $columns->folded()->schema(ColumnTypesMother::floor())->get('1')->name());
    }

    public function test_a_saturated_string_column_widens_to_optional_string_on_a_later_null(): void
    {
        $columns = ColumnTypesMother::fromStrings();
        $columns->observe(['c' => 'free text']);
        $columns->observe(['c' => null]);
        $columns->observe(['c' => 'more text']);

        static::assertEquals(
            new Schema(definition_from_type('c', type_optional(type_string()), nullable: true)),
            $columns->folded()->schema(ColumnTypesMother::floor()),
        );
    }

    public function test_a_source_with_no_names_and_no_rows_yields_an_empty_schema(): void
    {
        static::assertEquals(
            new Schema(),
            ColumnTypesMother::fromStrings()->folded()->schema(ColumnTypesMother::floor()),
        );
    }

    public function test_all_strings_floors_every_scalar_column(): void
    {
        $columns = ColumnTypesMother::fromStrings();
        $columns->observe(['a' => '1', 'b' => 'true', 'c' => '2024-01-01']);

        static::assertEquals(
            new Schema(
                definition_from_type('a', type_string(), nullable: true),
                definition_from_type('b', type_string(), nullable: true),
                definition_from_type('c', type_string(), nullable: true),
            ),
            $columns->folded()->schema(ColumnTypesMother::allStringsFloor()),
        );
    }

    public function test_an_absent_key_is_read_as_null(): void
    {
        $columns = ColumnTypesMother::fromStrings();
        $columns->observe(['a' => '1']);
        $columns->observe(['b' => 'x']);

        static::assertEquals(
            new Schema(
                definition_from_type('a', type_optional(type_integer()), nullable: true),
                definition_from_type('b', type_string(), nullable: true),
            ),
            $columns->folded()->schema(ColumnTypesMother::floor()),
        );
    }

    public function test_an_integer_column_is_declared_nullable(): void
    {
        $columns = ColumnTypesMother::fromStrings();
        $columns->observe(['c' => '1']);
        $columns->observe(['c' => '2']);

        $definition = $columns->folded()->schema(ColumnTypesMother::floor())->get('c');

        static::assertInstanceOf(Schema\Definition\IntegerDefinition::class, $definition);
        static::assertTrue($definition->isNullable());
    }

    public function test_an_optional_non_string_column_is_not_saturated(): void
    {
        $columns = ColumnTypesMother::fromStrings();
        $columns->observe(['c' => '1']);
        $columns->observe(['c' => null]);
        $columns->observe(['c' => '1.5']);

        static::assertEquals(
            new Schema(definition_from_type('c', type_optional(type_float()), nullable: true)),
            $columns->folded()->schema(ColumnTypesMother::floor()),
        );
    }

    public function test_from_column_types_reproduces_an_observed_fold(): void
    {
        $observed = ColumnTypesMother::fromStrings(['id', 'name']);
        $observed->observe(['id' => '1', 'name' => 'a']);
        $observed->observe(['id' => '2', 'name' => null]);

        $computed = ColumnTypesMother::fromColumnTypes([
            'id' => type_optional(type_integer()),
            'name' => type_optional(type_string()),
        ], 2);

        static::assertEquals(
            $observed->folded()->schema(ColumnTypesMother::floor()),
            $computed->folded()->schema(ColumnTypesMother::floor()),
        );
        static::assertSame($observed->rows(), $computed->rows());
    }

    public function test_null_never_reaches_the_ladder(): void
    {
        $columns = ColumnTypesMother::fromStrings();
        $columns->observe(['c' => '1']);
        $columns->observe(['c' => null]);
        $columns->observe(['c' => '2']);

        static::assertEquals(
            new Schema(definition_from_type('c', type_optional(type_integer()), nullable: true)),
            $columns->folded()->schema(ColumnTypesMother::floor()),
        );
    }

    public function test_observing_a_column_name_absent_from_the_header_still_records_its_type(): void
    {
        $columns = ColumnTypesMother::fromStrings(['a']);
        $columns->observe(['a' => 'free text', 'b' => '1']);
        $columns->observe(['a' => 'more text', 'b' => '2']);

        static::assertEquals(
            new Schema(
                definition_from_type('a', type_string(), nullable: true),
                definition_from_type('b', type_integer(), nullable: true),
            ),
            $columns->folded()->schema(ColumnTypesMother::floor()),
        );
    }

    public function test_padded_cells_are_read_as_text_because_cast_does_not_trim(): void
    {
        $padded = ColumnTypesMother::fromStrings();
        $padded->observe(['c' => ' 12 ']);
        $padded->observe(['c' => '13']);

        $clean = ColumnTypesMother::fromStrings();
        $clean->observe(['c' => '12']);
        $clean->observe(['c' => '13']);

        static::assertEquals(
            new Schema(definition_from_type('c', type_string(), nullable: true)),
            $padded->folded()->schema(ColumnTypesMother::floor()),
        );
        static::assertEquals(
            new Schema(definition_from_type('c', type_integer(), nullable: true)),
            $clean->folded()->schema(ColumnTypesMother::floor()),
        );
    }

    public function test_rows_counts_observations_not_columns(): void
    {
        $columns = ColumnTypesMother::fromStrings(['a', 'b']);

        static::assertSame(0, $columns->rows());

        $columns->observe(['a' => '1', 'b' => '2']);
        $columns->observe(['a' => '3']);
        $columns->observe([]);

        static::assertSame(3, $columns->rows());
    }

    /**
     * The goal, for the row that produced the type: cast() accepts the very cell the ladder saw.
     */
    #[DataProvider('ladderCells')]
    public function test_the_declared_type_fits_the_cell_that_produced_it(string $cell): void
    {
        $columns = ColumnTypesMother::fromStrings();
        $columns->observe(['c' => $cell]);

        $definition = $columns->folded()->schema(ColumnTypesMother::floor())->get('c');

        static::assertTrue($definition->matches($definition->type()->cast($cell)));
    }

    public function test_the_null_rung_is_read_as_text_because_the_cell_is_text(): void
    {
        $mixed = ColumnTypesMother::fromStrings();
        $mixed->observe(['c' => 'null']);
        $mixed->observe(['c' => '1']);

        $only = ColumnTypesMother::fromStrings();
        $only->observe(['c' => 'nil']);

        static::assertEquals(
            new Schema(definition_from_type('c', type_string(), nullable: true)),
            $mixed->folded()->schema(ColumnTypesMother::floor()),
        );
        static::assertEquals(
            new Schema(definition_from_type('c', type_string(), nullable: true)),
            $only->folded()->schema(ColumnTypesMother::floor()),
        );
    }

    /**
     * @param list<mixed> $values
     * @param Type<mixed> $expected
     */
    #[DataProvider('typedValueFolds')]
    public function test_typed_values_fold_through_the_instance_of_narrower(array $values, Type $expected): void
    {
        $columns = ColumnTypesMother::fromTypedValues();

        /** @var mixed $value */
        foreach ($values as $value) {
            $columns->observe(['c' => $value]);
        }

        static::assertEquals(
            new Schema(definition_from_type('c', $expected, nullable: true)),
            $columns->folded()->schema(ColumnTypesMother::floor()),
        );
    }

    /**
     * @param list<string> $values
     * @param Type<mixed> $expected
     */
    #[DataProvider('widenedColumns')]
    public function test_widening_across_rows_of_one_column(array $values, Type $expected): void
    {
        $columns = ColumnTypesMother::fromStrings();

        foreach ($values as $value) {
            $columns->observe(['c' => $value]);
        }

        static::assertEquals(
            new Schema(definition_from_type('c', $expected, nullable: true)),
            $columns->folded()->schema(ColumnTypesMother::floor()),
        );
    }

    public function test_sniff_observes_at_most_its_budget(): void
    {
        $sources = new RecordingSources([[['a' => '1'], ['a' => '2'], ['a' => '3'], ['a' => '4'], ['a' => '5']]]);

        static::assertSame(
            4,
            ColumnTypes::sniff(
                [],
                $sources->source(0),
                4,
                new StringTypeNarrower(InferredTypes::default()->toArray()),
            )->rows(),
        );
        static::assertSame([0 => 4], $sources->rowsRead);
    }

    public function test_sniff_seeds_the_names_it_is_handed_even_when_no_row_carries_them(): void
    {
        $partial = ColumnTypes::sniff(
            ['ghost'],
            [['a' => '1']],
            null,
            new StringTypeNarrower(InferredTypes::default()->toArray()),
        );

        static::assertSame(
            ['ghost', 'a'],
            array_keys($partial->folded()->schema(ColumnTypesMother::floor())->definitions()),
        );
        static::assertSame(
            'string',
            $partial->folded()->schema(ColumnTypesMother::floor())->get('ghost')->type()->toString(),
        );
    }

    public function test_sniff_observes_nothing_when_the_budget_is_already_spent(): void
    {
        $sources = new RecordingSources([[['a' => '1']]]);

        static::assertSame(
            0,
            ColumnTypes::sniff(
                [],
                $sources->source(0),
                0,
                new StringTypeNarrower(InferredTypes::default()->toArray()),
            )->rows(),
        );
        static::assertSame([], $sources->rowsRead);
    }

    public function test_sniff_returns_a_partial_equal_to_a_bare_fold_of_the_same_rows(): void
    {
        $rows = [['a' => '1', 'b' => 'x'], ['a' => '1.5', 'b' => 'y']];
        $bare = ColumnTypesMother::fromStrings();

        foreach ($rows as $row) {
            $bare->observe($row);
        }

        $partial = ColumnTypes::sniff([], $rows, null, new StringTypeNarrower(InferredTypes::default()->toArray()));

        static::assertSame(2, $partial->rows());
        static::assertEquals(
            $bare->folded()->schema(ColumnTypesMother::floor()),
            $partial->folded()->schema(ColumnTypesMother::floor()),
        );
    }

    public function test_a_negative_row_budget_is_refused_instead_of_read_as_a_budget(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Row budget must be 0 or more. Use null for every row.');

        // @mago-expect analysis:invalid-argument
        ColumnTypes::sniff([], [['a' => '1']], -1, new StringTypeNarrower(InferredTypes::default()->toArray()));
    }
}
