<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Rows;

use Flow\ETL\Column\PhpBackend;
use Flow\ETL\Exception\SchemaMismatchException;
use Flow\ETL\Rows\ColumnRetyping;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\ColumnMother;

use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_optional;

final class ColumnRetypingTest extends FlowTestCase
{
    public function test_an_absent_nullable_column_is_nulls(): void
    {
        static::assertSame(
            [null, null],
            (new ColumnRetyping())
                ->absent(int_schema('id', nullable: true), 2, new PhpBackend())
                ->values(),
        );
    }

    public function test_an_absent_not_null_column_is_refused_when_there_is_a_row(): void
    {
        $this->expectException(SchemaMismatchException::class);

        (new ColumnRetyping())->absent(int_schema('id'), 1, new PhpBackend());
    }

    public function test_an_absent_not_null_column_of_no_rows_is_empty(): void
    {
        static::assertSame(
            0,
            (new ColumnRetyping())
                ->absent(int_schema('id'), 0, new PhpBackend())
                ->count(),
        );
    }

    public function test_the_same_definition_keeps_the_column(): void
    {
        $column = ColumnMother::of(int_schema('id'), [1, 2]);

        static::assertSame($column, (new ColumnRetyping())->restamped($column, int_schema('id'), int_schema('id')));
    }

    public function test_a_change_the_types_cannot_prove_is_not_restamped(): void
    {
        static::assertNull((new ColumnRetyping())->restamped(
            ColumnMother::of(str_schema('id'), ['1']),
            str_schema('id'),
            int_schema('id'),
        ));
    }

    public function test_a_proved_change_is_restamped(): void
    {
        $retyped = (new ColumnRetyping())->restamped(
            ColumnMother::of(int_schema('id'), [1, 2]),
            int_schema('id'),
            int_schema('id', nullable: true),
        );

        static::assertNotNull($retyped);
        static::assertSame([1, 2], $retyped->values());
    }

    public function test_not_null_refuses_the_first_null_at_its_position(): void
    {
        try {
            (new ColumnRetyping())->notNull(
                int_schema('id'),
                ColumnMother::of(
                    int_schema('id', nullable: true),
                    [
                        1,
                        null,
                        null,
                    ],
                ),
            );
            static::fail('a null under NOT NULL must be refused');
        } catch (SchemaMismatchException $e) {
            static::assertSame(1, $e->rowIndex);
        }
    }

    public function test_cast_casts_what_the_types_cannot_prove(): void
    {
        static::assertSame(
            [1, 2],
            (new ColumnRetyping())
                ->cast(
                    ColumnMother::of(str_schema('id'), ['1', '2']),
                    str_schema('id'),
                    int_schema('id'),
                    new PhpBackend(),
                )
                ->values(),
        );
    }

    public function test_validate_refuses_the_lowest_bad_row_across_columns(): void
    {
        try {
            (new ColumnRetyping())->validate([
                'a' => [
                    ColumnMother::of(list_schema('a', type_list(type_optional(type_integer()))), [[1], [2], [null]]),
                    list_schema('a', type_list(type_integer())),
                ],
                'b' => [
                    ColumnMother::of(list_schema('b', type_list(type_optional(type_integer()))), [[1], [null], [3]]),
                    list_schema('b', type_list(type_integer())),
                ],
            ], 3);
            static::fail('a value the definition does not take must be refused');
        } catch (SchemaMismatchException $e) {
            static::assertSame(1, $e->rowIndex);
            static::assertStringContainsString('"b"', $e->getMessage());
        }
    }

    public function test_a_proved_change_with_a_null_under_not_null_is_left_to_validate(): void
    {
        static::assertNull((new ColumnRetyping())->restamped(
            ColumnMother::of(int_schema('id', nullable: true), [1, null]),
            int_schema('id', nullable: true),
            int_schema('id'),
        ));
    }
}
