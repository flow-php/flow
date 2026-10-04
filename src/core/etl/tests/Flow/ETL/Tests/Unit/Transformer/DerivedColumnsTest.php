<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use Flow\ETL\Column\PhpBackend;
use Flow\ETL\Column\ValueColumn;
use Flow\ETL\Exception\SchemaMismatchException;
use Flow\ETL\Tests\Double\SpyBackend;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\ColumnMother;
use Flow\ETL\Transformer\DerivedColumns;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class DerivedColumnsTest extends FlowTestCase
{
    public function test_declare_adds_a_new_column(): void
    {
        static::assertEquals(
            schema(int_schema('a'), str_schema('b')),
            (new DerivedColumns())->declare(schema(int_schema('a')), str_schema('b')),
        );
    }

    public function test_declare_replaces_an_existing_column(): void
    {
        static::assertEquals(
            schema(str_schema('a')),
            (new DerivedColumns())->declare(schema(int_schema('a')), str_schema('a')),
        );
    }

    public function test_stored_adopts_a_column_of_the_same_type(): void
    {
        $column = ColumnMother::of(int_schema('a', nullable: true), [1, 2]);
        $backend = new SpyBackend();

        $stored = (new DerivedColumns())->stored(int_schema('a'), $column, $backend);

        static::assertSame([1, 2], $stored->values());
        static::assertSame(1, $backend->adopts());
    }

    public function test_stored_rebuilds_a_column_of_another_type(): void
    {
        $stored = (new DerivedColumns())->stored(
            int_schema('a'),
            ColumnMother::of(str_schema('a'), ['5', '6']),
            new PhpBackend(),
        );

        static::assertSame([5, 6], $stored->values());
    }

    public function test_stored_rebuilds_an_untyped_column(): void
    {
        static::assertSame(
            [5, null],
            (new DerivedColumns())
                ->stored(int_schema('a', nullable: true), new ValueColumn(['5', null]), new PhpBackend())
                ->values(),
        );
    }

    public function test_stored_keeps_null_under_a_nullable_definition(): void
    {
        static::assertSame(
            [null],
            (new DerivedColumns())
                ->stored(
                    int_schema('a', nullable: true),
                    ColumnMother::of(int_schema('a', nullable: true), [null]),
                    new PhpBackend(),
                )
                ->values(),
        );
    }

    public function test_stored_names_the_row_of_a_null_under_a_not_null_definition(): void
    {
        $this->expectException(SchemaMismatchException::class);
        $this->expectExceptionMessage(
            'Rows do not match their schema: column "a" (row 3): could not convert null to integer, column is not nullable',
        );

        (new DerivedColumns())->stored(int_schema('a'), ColumnMother::of(int_schema('a', nullable: true), [
            1,
            2,
            3,
            null,
        ]), new PhpBackend());
    }

    public function test_rows_adds_the_derived_column_under_the_output_schema(): void
    {
        $output = schema(int_schema('a'), str_schema('note', nullable: true));

        static::assertSame(
            [['a' => 1, 'note' => 'x']],
            (new DerivedColumns())
                ->rows(
                    array_to_rows([['a' => 1]], schema(int_schema('a'))),
                    $output,
                    $output,
                    'note',
                    (new PhpBackend())->constant(str_schema('note', nullable: true), 'x', 1),
                    new PhpBackend(),
                )
                ->toArray(),
        );
    }

    public function test_rows_matches_a_batch_declared_under_another_schema(): void
    {
        $declared = schema(int_schema('a'), str_schema('note', nullable: true));

        static::assertSame(
            [['a' => 1, 'note' => 'x', 'extra' => null]],
            (new DerivedColumns())
                ->rows(
                    array_to_rows([['a' => 1]], schema(int_schema('a'))),
                    $declared,
                    $declared->add(str_schema('extra', nullable: true)),
                    'note',
                    (new PhpBackend())->constant(str_schema('note', nullable: true), 'x', 1),
                    new PhpBackend(),
                )
                ->toArray(),
        );
    }
}
