<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\IO\Inference;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\IO\Inference\SniffedColumns;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\ColumnTypesMother;
use Flow\ETL\Values\Schema;
use Flow\Types\Type;
use PHPUnit\Framework\Attributes\DataProvider;

use function array_keys;
use function Flow\ETL\DSL\definition_from_type;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_equals;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;

final class SniffedColumnsTest extends FlowTestCase
{
    public function test_negative_rows_are_rejected(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Sniffed rows must not be negative');

        // @mago-expect analysis:invalid-argument
        new SniffedColumns([], -1);
    }

    public function test_normalize_round_trips_a_numeric_column_name(): void
    {
        $columns = ColumnTypesMother::fromStrings();
        $columns->observe(['1' => '7', 'name' => 'x']);
        $folded = $columns->folded();

        static::assertSame(
            [
                ['name' => '1', 'type' => type_integer()->normalize()],
                ['name' => 'name', 'type' => type_string()->normalize()],
            ],
            $folded->normalize()['columns'],
        );
        static::assertSame(1, $folded->normalize()['rows']);

        $rebuilt = SniffedColumns::fromArray($folded->normalize());

        static::assertEquals($folded->schema(ColumnTypesMother::floor()), $rebuilt->schema(ColumnTypesMother::floor()));
        static::assertSame($folded->rows, $rebuilt->rows);
        static::assertSame('1', $rebuilt->schema(ColumnTypesMother::floor())->get('1')->name());
    }

    public function test_normalize_round_trips_every_default_candidate(): void
    {
        $columns = ColumnTypesMother::fromStrings();
        $columns->observe([
            'i' => '1',
            'f' => '1.5',
            'b' => 'true',
            'd' => '2024-01-01',
            'dt' => '2024-01-01 10:00:00',
            'j' => '{"a":1}',
            's' => 'text',
        ]);
        $folded = $columns->folded();

        static::assertEquals(
            $folded->schema(ColumnTypesMother::floor()),
            SniffedColumns::fromArray($folded->normalize())->schema(ColumnTypesMother::floor()),
        );
    }

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

    public function test_from_column_types_merges_with_an_observed_fold(): void
    {
        $left = ColumnTypesMother::fromStrings(['a']);
        $left->observe(['a' => '1']);
        $right = ColumnTypesMother::fromStrings(['a']);
        $right->observe(['a' => '1.5', 'b' => 'x']);

        static::assertEquals(
            $left->folded()->merge($right->folded(), true)->schema(ColumnTypesMother::floor()),
            $left
                ->folded()
                ->merge(
                    ColumnTypesMother::fromColumnTypes([
                        'a' => type_optional(type_float()),
                        'b' => type_string(),
                    ], 1)->folded(),
                    true,
                )
                ->schema(ColumnTypesMother::floor()),
        );
    }

    public function test_from_column_types_carries_the_row_count_into_merge(): void
    {
        $observed = ColumnTypesMother::fromStrings();
        $observed->observe(['a' => '1']);

        static::assertSame(
            8,
            $observed->folded()->merge(
                ColumnTypesMother::fromColumnTypes(['a' => type_integer()], 7)->folded(),
                true,
            )->rows,
        );
    }

    public function test_merge_is_associative_over_three_partials(): void
    {
        $left = ColumnTypesMother::fromStrings(['a']);
        $left->observe(['a' => '1']);
        $middle = ColumnTypesMother::fromStrings(['b']);
        $middle->observe(['b' => 'x']);
        $right = ColumnTypesMother::fromStrings(['c']);
        $right->observe(['a' => '1.5', 'c' => 'true']);

        $leftAssociated = $left
            ->folded()
            ->merge($middle->folded(), true)
            ->merge($right->folded(), true)
            ->schema(ColumnTypesMother::floor());
        $rightAssociated = $left
            ->folded()
            ->merge($middle->folded()->merge($right->folded(), true), true)
            ->schema(ColumnTypesMother::floor());

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
            array_keys(
                $left->folded()->merge($right->folded(), true)->schema(ColumnTypesMother::floor())->definitions(),
            ),
        );
        static::assertSame(
            ['c', 'b', 'a'],
            array_keys(
                $right->folded()->merge($left->folded(), true)->schema(ColumnTypesMother::floor())->definitions(),
            ),
        );
    }

    public function test_merge_sums_the_row_counts_and_leaves_both_operands_alone(): void
    {
        $left = ColumnTypesMother::fromStrings();
        $left->observe(['a' => '1']);
        $left->observe(['a' => '2']);
        $right = ColumnTypesMother::fromStrings();
        $right->observe(['a' => '3']);

        static::assertSame(3, $left->folded()->merge($right->folded(), true)->rows);
        static::assertSame(2, $left->rows());
        static::assertSame(1, $right->rows());
    }

    public function test_merge_takes_a_seeded_only_partial_as_the_identity_of_the_fold(): void
    {
        $seeded = ColumnTypesMother::fromStrings(['a', 'b']);
        $observed = ColumnTypesMother::fromStrings(['a', 'b']);
        $observed->observe(['a' => '1']);

        $expected = new Schema(
            definition_from_type('a', type_integer(), nullable: true),
            definition_from_type('b', type_string(), nullable: true),
        );

        static::assertEquals(
            $expected,
            $seeded->folded()->merge($observed->folded(), true)->schema(ColumnTypesMother::floor()),
        );
        static::assertEquals(
            $expected,
            $observed->folded()->merge($seeded->folded(), true)->schema(ColumnTypesMother::floor()),
        );
    }

    public function test_merge_unions_disjoint_name_sets(): void
    {
        $left = ColumnTypesMother::fromStrings();
        $left->observe(['a' => '1']);
        $right = ColumnTypesMother::fromStrings();
        $right->observe(['b' => 'x']);

        static::assertEquals(
            new Schema(
                definition_from_type('a', type_integer(), nullable: true),
                definition_from_type('b', type_string(), nullable: true),
            ),
            $left->folded()->merge($right->folded(), true)->schema(ColumnTypesMother::floor()),
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
        $left->observe(['c' => $leftValue]);
        $right = ColumnTypesMother::fromStrings();
        $right->observe(['c' => $rightValue]);

        $leftFirst = $left
            ->folded()
            ->merge($right->folded(), true)
            ->schema(ColumnTypesMother::floor())
            ->get('c')
            ->type();
        $rightFirst = $right
            ->folded()
            ->merge($left->folded(), true)
            ->schema(ColumnTypesMother::floor())
            ->get('c')
            ->type();

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
        $left->observe(['c' => []]);
        $right = ColumnTypesMother::fromTypedValues();
        $right->observe(['c' => [1]]);

        static::assertTrue(type_equals(
            type_list(type_optional(type_integer())),
            $left->folded()->merge($right->folded(), true)->schema(ColumnTypesMother::floor())->get('c')->type(),
        ));
        static::assertTrue(type_equals(
            $left->folded()->merge($right->folded(), true)->schema(ColumnTypesMother::floor())->get('c')->type(),
            $right->folded()->merge($left->folded(), true)->schema(ColumnTypesMother::floor())->get('c')->type(),
        ));
    }
}
