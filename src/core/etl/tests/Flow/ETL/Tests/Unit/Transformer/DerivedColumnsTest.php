<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use Flow\ETL\Exception\SchemaMismatchException;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Transformer\DerivedColumns;

use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\row;
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

    public function test_value_casts_into_the_declared_type(): void
    {
        static::assertSame(5, (new DerivedColumns())->value(int_schema('a'), '5', 0));
    }

    public function test_value_keeps_null_under_a_nullable_definition(): void
    {
        static::assertNull((new DerivedColumns())->value(int_schema('a', nullable: true), null, 0));
    }

    public function test_value_names_the_row_of_a_null_under_a_not_null_definition(): void
    {
        $this->expectException(SchemaMismatchException::class);
        $this->expectExceptionMessage(
            'Rows do not match their schema: column "a" (row 3): could not convert null to integer, column is not nullable',
        );

        (new DerivedColumns())->value(int_schema('a'), null, 3);
    }

    public function test_rows_trusts_a_batch_declared_under_the_output_schema(): void
    {
        $output = schema(int_schema('a'), str_schema('note', nullable: true));

        static::assertSame(
            [['a' => 1]],
            (new DerivedColumns())
                ->rows($output, $output, [row(['a' => 1])])
                ->toArray(),
        );
    }

    public function test_rows_conforms_a_batch_declared_under_another_schema(): void
    {
        static::assertSame(
            [['a' => 1, 'note' => null]],
            (new DerivedColumns())
                ->rows(schema(int_schema('a')), schema(int_schema('a'), str_schema('note', nullable: true)), [row([
                    'a' => 1,
                ])])
                ->toArray(),
        );
    }
}
