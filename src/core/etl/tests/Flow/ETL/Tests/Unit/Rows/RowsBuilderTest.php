<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Rows;

use Flow\ETL\Column\PhpBackend;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\SchemaMismatchException;
use Flow\ETL\Rows;
use Flow\ETL\Rows\RowsBuilder;
use Flow\ETL\Schema;
use Flow\ETL\Tests\FlowTestCase;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class RowsBuilderTest extends FlowTestCase
{
    /**
     * @return Generator<string, array{Rows, Schema, string}>
     */
    public static function refused_views(): Generator
    {
        yield 'a not null column the source lacks' => [
            rows(schema(int_schema('id')), row(['id' => 1])),
            schema(int_schema('id'), str_schema('name')),
            'column "name" (row 0) declared by the schema is missing from the row',
        ];
        yield 'null under not null' => [
            rows(schema(int_schema('id', nullable: true)), row(['id' => null])),
            schema(int_schema('id')),
            'column "id" (row 0): could not convert null to integer, column is not nullable',
        ];
        yield 'a value that matches only after a cast' => [
            rows(schema(str_schema('id')), row(['id' => '2'])),
            schema(int_schema('id')),
            'column "id" (row 0): could not convert \'2\' (string) to integer',
        ];
        yield 'a column the target does not declare' => [
            rows(schema(int_schema('id'), int_schema('extra')), row(['id' => 1, 'extra' => 2])),
            schema(int_schema('id')),
            'column "extra" (row 0) is not declared by the schema',
        ];
    }

    public function test_a_refused_value_wins_over_an_earlier_absence(): void
    {
        $this->expectException(SchemaMismatchException::class);
        $this->expectExceptionMessage('column "b" (row 1): could not convert null to integer, column is not nullable');

        (new RowsBuilder(schema(int_schema('a'), int_schema('b')), new PhpBackend()))->appendRows([
            ['a' => 1],
            ['a' => 1, 'b' => null],
        ]);
    }

    public function test_an_absence_is_placed_after_the_rows_already_appended(): void
    {
        $this->expectException(SchemaMismatchException::class);
        $this->expectExceptionMessage('column "c" (row 2) declared by the schema is missing from the row');

        (new RowsBuilder(
            schema(int_schema('a'), int_schema('b', nullable: true), int_schema('c')),
            new PhpBackend(),
        ))->appendRows([['a' => 0, 'c' => 0]])->appendRows([['a' => 1, 'c' => 1], ['a' => 2]]);
    }

    public function test_the_first_refused_value_in_row_order_across_columns(): void
    {
        $this->expectException(SchemaMismatchException::class);
        $this->expectExceptionMessage('column "b" (row 1): could not convert \'x\' (string) to integer');

        (new RowsBuilder(schema(int_schema('a'), int_schema('b')), new PhpBackend()))->appendRows([[
            'a' => 0,
            'b' => 0,
        ]])->appendRows([['a' => 1, 'b' => 'x'], ['a' => 'y', 'b' => 2]]);
    }

    public function test_a_view_of_another_definition_keeps_a_value_that_matches_the_target(): void
    {
        static::assertSame(
            [['id' => 5, 'name' => null]],
            (new RowsBuilder(schema(int_schema('id'), str_schema('name', nullable: true)), new PhpBackend()))
                ->appendFrom(rows(schema(int_schema('id', nullable: true)), row(['id' => 5])), 0)
                ->finish()
                ->toArray(),
        );
    }

    public function test_a_refused_view_is_placed_at_its_output_row(): void
    {
        $this->expectException(SchemaMismatchException::class);
        $this->expectExceptionMessage('column "id" (row 1): could not convert \'x\' (string) to integer');

        (new RowsBuilder(schema(int_schema('id')), new PhpBackend()))
            ->appendFrom(rows(schema(int_schema('id')), row(['id' => 1])), 0)
            ->appendFrom(rows(schema(str_schema('id')), row(['id' => 'x'])), 0);
    }

    #[DataProvider('refused_views')]
    public function test_refuses_a_view_that_does_not_fit_the_target(
        Rows $source,
        Schema $target,
        string $message,
    ): void {
        $this->expectException(SchemaMismatchException::class);
        $this->expectExceptionMessage($message);

        (new RowsBuilder($target, new PhpBackend()))->appendFrom($source, 0);
    }

    public function test_a_missing_nullable_column_is_padded_with_null(): void
    {
        static::assertEquals(
            rows(schema(int_schema('id'), str_schema('name', nullable: true)), row(['id' => 1, 'name' => null])),
            (new RowsBuilder(
                schema(int_schema('id'), str_schema('name', nullable: true)),
                new PhpBackend(),
            ))->appendRows([['id' => 1]])->finish(),
        );
    }

    public function test_a_missing_not_null_column_is_refused_with_its_row(): void
    {
        $this->expectException(SchemaMismatchException::class);
        $this->expectExceptionMessage(
            'Rows do not match their schema: column "name" (row 1) declared by the schema is missing from the row',
        );

        (new RowsBuilder(schema(int_schema('id'), str_schema('name')), new PhpBackend()))->appendRows([
            ['id' => 1, 'name' => 'a'],
            ['id' => 2],
        ]);
    }

    public function test_a_refused_value_in_the_first_row_reports_row_zero(): void
    {
        $this->expectException(SchemaMismatchException::class);
        $this->expectExceptionMessage(
            'Rows do not match their schema: column "i" (row 0): could not convert \'abc\' (string) to integer',
        );

        (new RowsBuilder(schema(int_schema('i')), new PhpBackend()))->appendRows([['i' => 'abc']]);
    }

    public function test_a_refused_value_is_reported_with_its_column_and_row(): void
    {
        $this->expectException(SchemaMismatchException::class);
        $this->expectExceptionMessage(
            'Rows do not match their schema: column "i" (row 1): could not convert \'n/a\' (string) to integer',
        );

        (new RowsBuilder(schema(int_schema('i')), new PhpBackend()))->appendRows([
            ['i' => 1],
            ['i' => 'n/a'],
            ['i' => 3],
        ]);
    }

    public function test_a_refusal_in_a_later_append_reports_its_row_in_the_whole_batch(): void
    {
        $builder = new RowsBuilder(schema(int_schema('i')), new PhpBackend());
        $builder->appendRows([['i' => 1], ['i' => 2]]);

        $this->expectException(SchemaMismatchException::class);
        $this->expectExceptionMessage('column "i" (row 3): could not convert \'x\' (string) to integer');

        $builder->appendRows([['i' => 3], ['i' => 'x']]);
    }

    public function test_a_null_on_a_not_null_column_is_reported_as_a_value_that_does_not_match(): void
    {
        $this->expectException(SchemaMismatchException::class);
        $this->expectExceptionMessage(
            'Rows do not match their schema: column "i" (row 0): could not convert null to integer, column is not nullable',
        );

        (new RowsBuilder(schema(int_schema('i')), new PhpBackend()))->append(['i' => null]);
    }

    public function test_a_value_the_schema_does_not_declare_is_dropped(): void
    {
        static::assertSame(
            ['id'],
            (new RowsBuilder(schema(int_schema('id')), new PhpBackend()))
                ->append(['id' => 1, 'undeclared' => 'x'])
                ->finish()
                ->first()
                ->names(),
        );
    }

    public function test_rows_follow_the_schema_column_order(): void
    {
        static::assertSame(
            [['id' => 1, 'name' => 'a']],
            (new RowsBuilder(schema(int_schema('id'), str_schema('name')), new PhpBackend()))
                ->append(['name' => 'a', 'id' => 1])
                ->finish()
                ->toArray(),
        );
    }

    public function test_every_value_is_cast_against_its_own_column(): void
    {
        static::assertEquals(
            rows(schema(int_schema('id'), str_schema('name')), row(['id' => 1, 'name' => '2'])),
            (new RowsBuilder(schema(int_schema('id'), str_schema('name')), new PhpBackend()))->append([
                'id' => '1',
                'name' => 2,
            ])->finish(),
        );
    }

    public function test_a_schema_without_definitions_counts_rows(): void
    {
        $builder = new RowsBuilder(schema(), new PhpBackend());
        $builder->append([]);
        $builder->append([]);
        $builder->appendRows([[]]);

        static::assertSame(3, $builder->count());
        static::assertSame(3, $builder->finish()->count());
    }

    public function test_appending_no_rows_changes_nothing(): void
    {
        static::assertSame(0, (new RowsBuilder(schema(int_schema('id')), new PhpBackend()))->appendRows([])->count());
    }

    public function test_copies_physical_cells_from_another_batch(): void
    {
        $source = rows(
            schema(int_schema('id'), str_schema('name')),
            row(['id' => 1, 'name' => 'a']),
            row([
                'id' => 2,
                'name' => 'b',
            ]),
        );

        static::assertSame(
            [['id' => 2, 'name' => 'b'], ['id' => 2, 'name' => 'b'], ['id' => 1, 'name' => 'a']],
            (new RowsBuilder(schema(int_schema('id'), str_schema('name')), new PhpBackend()))
                ->appendFrom($source, 1)
                ->appendTake($source, [1, 0])
                ->finish()
                ->toArray(),
        );
    }

    public function test_feeds_one_column(): void
    {
        $builder = new RowsBuilder(schema(int_schema('id')), new PhpBackend());
        $builder->column('id')->appendMany([1, 2]);

        static::assertSame(2, $builder->count());
        static::assertSame([['id' => 1], ['id' => 2]], $builder->finish()->toArray());
    }

    public function test_an_unknown_column_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('RowsBuilder has no column "nope"');

        (new RowsBuilder(schema(int_schema('id')), new PhpBackend()))->column('nope');
    }

    public function test_ragged_columns_are_refused_at_finish(): void
    {
        $builder = new RowsBuilder(schema(int_schema('a'), int_schema('b')), new PhpBackend());
        $builder->column('a')->appendMany([1, 2]);
        $builder->column('b')->append(1);

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Ragged columns: 2 rows in [a], 1 rows in [b]');

        $builder->finish();
    }
}
