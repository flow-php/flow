<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Schema\Inference;

use Flow\ETL\Row\RawRowValues;
use Flow\ETL\Schema;
use Flow\ETL\Schema\Metadata;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\ColumnTypesMother;
use Flow\Types\Type;
use PHPUnit\Framework\Attributes\DataProvider;

use function array_keys;
use function Flow\ETL\DSL\definition_from_type;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_equals;
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
     * @return array<string, array{string, string, Type<mixed>}>
     */
    public static function commutativityPairs(): array
    {
        return [
            'integer / float' => ['1', '1.5', type_float()],
            'integer / text' => ['1', 'x', type_string()],
            'date / datetime' => ['2024-01-01', '2024-01-01 10:00', type_datetime()],
            'integer / the null rung' => ['1', 'null', type_string()],
        ];
    }

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
        ];
    }

    public function test_a_header_only_source_yields_every_name_as_nullable_string(): void
    {
        $schema = ColumnTypesMother::fromStrings(['id', 'name'])->schema(ColumnTypesMother::floor());

        static::assertEquals(
            new Schema(
                definition_from_type('id', type_string(), nullable: true),
                definition_from_type('name', type_string(), nullable: true),
            ),
            $schema,
        );
    }

    public function test_a_name_met_only_in_a_row_is_appended_after_the_header_names(): void
    {
        $columns = ColumnTypesMother::fromStrings(['a', 'b']);
        $columns->observe(new RawRowValues(['c' => '1']));

        static::assertSame(['a', 'b', 'c'], array_keys($columns->schema(ColumnTypesMother::floor())->definitions()));
    }

    public function test_a_numeric_key_becomes_a_string_column_name(): void
    {
        $columns = ColumnTypesMother::fromStrings();
        // PHP re-keys a numeric column name to int; RawRowValues declares array<string, mixed>, which cannot
        // express that, so the fold has to survive an int key arriving through a documented-string array
        // @mago-ignore analysis:possibly-invalid-argument
        $columns->observe(new RawRowValues(['1' => 'x']));

        static::assertSame('1', $columns->schema(ColumnTypesMother::floor())->get('1')->entry()->name());
    }

    public function test_a_source_with_no_names_and_no_rows_yields_an_empty_schema(): void
    {
        static::assertEquals(new Schema(), ColumnTypesMother::fromStrings()->schema(ColumnTypesMother::floor()));
    }

    public function test_all_strings_floors_every_scalar_column(): void
    {
        $columns = ColumnTypesMother::fromStrings();
        $columns->observe(new RawRowValues(['a' => '1', 'b' => 'true', 'c' => '2024-01-01']));

        static::assertEquals(
            new Schema(
                definition_from_type('a', type_string(), nullable: true),
                definition_from_type('b', type_string(), nullable: true),
                definition_from_type('c', type_string(), nullable: true),
            ),
            $columns->schema(ColumnTypesMother::allStringsFloor()),
        );
    }

    public function test_an_absent_key_is_read_as_null(): void
    {
        $columns = ColumnTypesMother::fromStrings();
        $columns->observe(new RawRowValues(['a' => '1']));
        $columns->observe(new RawRowValues(['b' => 'x']));

        static::assertEquals(
            new Schema(
                definition_from_type('a', type_optional(type_integer()), nullable: true),
                definition_from_type('b', type_string(), nullable: true),
            ),
            $columns->schema(ColumnTypesMother::floor()),
        );
    }

    public function test_an_integer_column_is_declared_nullable(): void
    {
        $columns = ColumnTypesMother::fromStrings();
        $columns->observe(new RawRowValues(['c' => '1']));
        $columns->observe(new RawRowValues(['c' => '2']));

        $definition = $columns->schema(ColumnTypesMother::floor())->get('c');

        static::assertInstanceOf(Schema\Definition\IntegerDefinition::class, $definition);
        static::assertTrue($definition->isNullable());
    }

    public function test_merge_is_associative_over_three_partials(): void
    {
        $left = ColumnTypesMother::fromStrings(['a']);
        $left->observe(new RawRowValues(['a' => '1']));
        $middle = ColumnTypesMother::fromStrings(['b']);
        $middle->observe(new RawRowValues(['b' => 'x']));
        $right = ColumnTypesMother::fromStrings(['c']);
        $right->observe(new RawRowValues(['a' => '1.5', 'c' => 'true']));

        $leftAssociated = $left->merge($middle, true)->merge($right, true)->schema(ColumnTypesMother::floor());
        $rightAssociated = $left->merge($middle->merge($right, true), true)->schema(ColumnTypesMother::floor());

        static::assertEquals($leftAssociated, $rightAssociated);
        static::assertSame(['a', 'b', 'c'], array_keys($leftAssociated->definitions()));
        static::assertSame(['a', 'b', 'c'], array_keys($rightAssociated->definitions()));
        static::assertEquals(
            new Schema(
                definition_from_type('a', type_float(), nullable: true),
                definition_from_type('b', type_string(), nullable: true),
                definition_from_type('c', type_boolean(), nullable: true),
            ),
            $leftAssociated,
        );
    }

    public function test_merge_keeps_the_left_operands_name_order_then_appends_the_rights(): void
    {
        $left = ColumnTypesMother::fromStrings(['b', 'a']);
        $right = ColumnTypesMother::fromStrings(['c']);

        static::assertSame(
            ['b', 'a', 'c'],
            array_keys($left->merge($right, true)->schema(ColumnTypesMother::floor())->definitions()),
        );
        static::assertSame(
            ['c', 'b', 'a'],
            array_keys($right->merge($left, true)->schema(ColumnTypesMother::floor())->definitions()),
        );
    }

    public function test_merge_sums_the_row_counts_and_leaves_both_operands_alone(): void
    {
        $left = ColumnTypesMother::fromStrings();
        $left->observe(new RawRowValues(['a' => '1']));
        $left->observe(new RawRowValues(['a' => '2']));
        $right = ColumnTypesMother::fromStrings();
        $right->observe(new RawRowValues(['a' => '3']));

        static::assertSame(3, $left->merge($right, true)->rows());
        static::assertSame(2, $left->rows());
        static::assertSame(1, $right->rows());
    }

    public function test_merge_takes_a_seeded_only_partial_as_the_identity_of_the_fold(): void
    {
        $seeded = ColumnTypesMother::fromStrings(['a', 'b']);
        $observed = ColumnTypesMother::fromStrings(['a', 'b']);
        $observed->observe(new RawRowValues(['a' => '1']));

        $expected = new Schema(
            definition_from_type('a', type_integer(), nullable: true),
            definition_from_type('b', type_string(), nullable: true),
        );

        static::assertEquals($expected, $seeded->merge($observed, true)->schema(ColumnTypesMother::floor()));
        static::assertEquals($expected, $observed->merge($seeded, true)->schema(ColumnTypesMother::floor()));
    }

    public function test_merge_unions_disjoint_name_sets(): void
    {
        $left = ColumnTypesMother::fromStrings();
        $left->observe(new RawRowValues(['a' => '1']));
        $right = ColumnTypesMother::fromStrings();
        $right->observe(new RawRowValues(['b' => 'x']));

        static::assertEquals(
            new Schema(
                definition_from_type('a', type_integer(), nullable: true),
                definition_from_type('b', type_string(), nullable: true),
            ),
            $left->merge($right, true)->schema(ColumnTypesMother::floor()),
        );
    }

    /**
     * @param Type<mixed> $expected
     */
    #[DataProvider('commutativityPairs')]
    public function test_merged_types_do_not_depend_on_the_order_of_the_two_partials(
        string $leftValue,
        string $rightValue,
        Type $expected,
    ): void {
        $left = ColumnTypesMother::fromStrings();
        $left->observe(new RawRowValues(['c' => $leftValue]));
        $right = ColumnTypesMother::fromStrings();
        $right->observe(new RawRowValues(['c' => $rightValue]));

        $leftFirst = $left->merge($right, true)->schema(ColumnTypesMother::floor())->get('c')->type();
        $rightFirst = $right->merge($left, true)->schema(ColumnTypesMother::floor())->get('c')->type();

        static::assertTrue(
            type_equals($expected, $leftFirst),
            $expected->toString() . ' !== ' . $leftFirst->toString(),
        );
        static::assertTrue(
            type_equals($expected, $rightFirst),
            $expected->toString() . ' !== ' . $rightFirst->toString(),
        );
    }

    public function test_merged_types_do_not_depend_on_the_order_for_container_partials(): void
    {
        $left = ColumnTypesMother::fromTypedValues();
        $left->observe(new RawRowValues(['c' => []]));
        $right = ColumnTypesMother::fromTypedValues();
        $right->observe(new RawRowValues(['c' => [1]]));

        static::assertTrue(type_equals(
            type_list(type_optional(type_integer())),
            $left->merge($right, true)->schema(ColumnTypesMother::floor())->get('c')->type(),
        ));
        static::assertTrue(type_equals(
            $left->merge($right, true)->schema(ColumnTypesMother::floor())->get('c')->type(),
            $right->merge($left, true)->schema(ColumnTypesMother::floor())->get('c')->type(),
        ));
    }

    public function test_metadata_never_reaches_the_schema(): void
    {
        $columns = ColumnTypesMother::fromStrings();
        $columns->observe(new RawRowValues(['a' => '1'], ['a' => Metadata::empty()->add('k', 'v')]));

        static::assertEquals(
            new Schema(definition_from_type('a', type_integer(), nullable: true)),
            $columns->schema(ColumnTypesMother::floor()),
        );
    }

    public function test_null_never_reaches_the_ladder(): void
    {
        $columns = ColumnTypesMother::fromStrings();
        $columns->observe(new RawRowValues(['c' => '1']));
        $columns->observe(new RawRowValues(['c' => null]));
        $columns->observe(new RawRowValues(['c' => '2']));

        static::assertEquals(
            new Schema(definition_from_type('c', type_optional(type_integer()), nullable: true)),
            $columns->schema(ColumnTypesMother::floor()),
        );
    }

    public function test_padded_cells_are_read_as_text_because_cast_does_not_trim(): void
    {
        $padded = ColumnTypesMother::fromStrings();
        $padded->observe(new RawRowValues(['c' => ' 12 ']));
        $padded->observe(new RawRowValues(['c' => '13']));

        $clean = ColumnTypesMother::fromStrings();
        $clean->observe(new RawRowValues(['c' => '12']));
        $clean->observe(new RawRowValues(['c' => '13']));

        static::assertEquals(
            new Schema(definition_from_type('c', type_string(), nullable: true)),
            $padded->schema(ColumnTypesMother::floor()),
        );
        static::assertEquals(
            new Schema(definition_from_type('c', type_integer(), nullable: true)),
            $clean->schema(ColumnTypesMother::floor()),
        );
    }

    public function test_rows_counts_observations_not_columns(): void
    {
        $columns = ColumnTypesMother::fromStrings(['a', 'b']);

        static::assertSame(0, $columns->rows());

        $columns->observe(new RawRowValues(['a' => '1', 'b' => '2']));
        $columns->observe(new RawRowValues(['a' => '3']));
        $columns->observe(new RawRowValues([]));

        static::assertSame(3, $columns->rows());
    }

    /**
     * The goal, for the row that produced the type: cast() accepts the very cell the ladder saw.
     */
    #[DataProvider('ladderCells')]
    public function test_the_declared_type_fits_the_cell_that_produced_it(string $cell): void
    {
        $columns = ColumnTypesMother::fromStrings();
        $columns->observe(new RawRowValues(['c' => $cell]));

        $definition = $columns->schema(ColumnTypesMother::floor())->get('c');

        static::assertTrue($definition->matches($definition->type()->cast($cell)));
    }

    public function test_the_null_rung_is_read_as_text_because_the_cell_is_text(): void
    {
        $mixed = ColumnTypesMother::fromStrings();
        $mixed->observe(new RawRowValues(['c' => 'null']));
        $mixed->observe(new RawRowValues(['c' => '1']));

        $only = ColumnTypesMother::fromStrings();
        $only->observe(new RawRowValues(['c' => 'nil']));

        static::assertEquals(
            new Schema(definition_from_type('c', type_string(), nullable: true)),
            $mixed->schema(ColumnTypesMother::floor()),
        );
        static::assertEquals(
            new Schema(definition_from_type('c', type_string(), nullable: true)),
            $only->schema(ColumnTypesMother::floor()),
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
            $columns->observe(new RawRowValues(['c' => $value]));
        }

        static::assertEquals(
            new Schema(definition_from_type('c', $expected, nullable: true)),
            $columns->schema(ColumnTypesMother::floor()),
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
            $columns->observe(new RawRowValues(['c' => $value]));
        }

        static::assertEquals(
            new Schema(definition_from_type('c', $expected, nullable: true)),
            $columns->schema(ColumnTypesMother::floor()),
        );
    }
}
