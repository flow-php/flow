<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row;

use Flow\ETL\Exception\SchemaMismatchException;
use Flow\ETL\Row\HydratedBatch;
use Flow\ETL\Row\RawRowValues;
use Flow\ETL\Schema\Definition;
use Flow\ETL\Schema\Metadata;
use Flow\ETL\Tests\FlowTestCase;
use LogicException;

use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class HydratedBatchTest extends FlowTestCase
{
    public function test_a_missing_column_is_filled_with_null_when_asked(): void
    {
        static::assertEquals(
            rows(schema(int_schema('id'), str_schema('name', nullable: true)), row(['id' => 1, 'name' => null])),
            (new HydratedBatch())->of(
                [new RawRowValues(['id' => 1])],
                schema(int_schema('id'), str_schema('name', nullable: true)),
                static fn(mixed $value, Definition $definition): mixed => $value,
                true,
            ),
        );
    }

    public function test_a_missing_nullable_column_is_padded_by_the_batch_door(): void
    {
        static::assertSame(
            ['id', 'name'],
            (new HydratedBatch())
                ->of(
                    [new RawRowValues(['id' => 1])],
                    schema(int_schema('id'), str_schema('name', nullable: true)),
                    static fn(mixed $value, Definition $definition): mixed => $value,
                    false,
                )
                ->first()
                ->names(),
        );
    }

    public function test_a_refused_value_in_the_first_row_reports_row_zero(): void
    {
        $this->expectException(SchemaMismatchException::class);
        $this->expectExceptionMessage(
            'Rows do not match their schema: column "i" (row 0): could not convert \'abc\' (string) to integer',
        );

        (new HydratedBatch())->of(
            [new RawRowValues(['i' => 'abc'])],
            schema(int_schema('i')),
            static fn(mixed $value, Definition $definition): mixed => $definition->type()->cast($value),
            false,
        );
    }

    public function test_a_value_the_identity_prepare_lets_through_is_still_refused_by_the_rows_gate(): void
    {
        $this->expectException(SchemaMismatchException::class);
        $this->expectExceptionMessage(
            'Rows do not match their schema: column "i" (row 1): could not convert \'abc\' (string) to integer',
        );

        (new HydratedBatch())->of(
            [new RawRowValues(['i' => 1]), new RawRowValues(['i' => 'abc'])],
            schema(int_schema('i')),
            static fn(mixed $value, Definition $definition): mixed => $value,
            false,
        );
    }

    public function test_a_value_the_prepare_callback_refuses_is_reported_with_its_column_and_row(): void
    {
        $this->expectException(SchemaMismatchException::class);
        $this->expectExceptionMessage(
            'Rows do not match their schema: column "i" (row 1): could not convert \'n/a\' (string) to integer',
        );

        (new HydratedBatch())->of(
            [new RawRowValues(['i' => 1]), new RawRowValues(['i' => 'n/a']), new RawRowValues(['i' => 3])],
            schema(int_schema('i')),
            static fn(mixed $value, Definition $definition): mixed => $definition->type()->cast($value),
            false,
        );
    }

    public function test_a_value_the_prepare_callback_refuses_with_a_non_types_exception_is_not_wrapped(): void
    {
        $this->expectException(LogicException::class);
        $this->expectExceptionMessage('not a casting failure');

        (new HydratedBatch())->of(
            [new RawRowValues(['i' => 1])],
            schema(int_schema('i')),
            static function (mixed $value, Definition $definition): mixed {
                throw new LogicException('not a casting failure');
            },
            false,
        );
    }

    public function test_a_value_the_schema_does_not_declare_is_dropped(): void
    {
        static::assertSame(
            ['id'],
            (new HydratedBatch())
                ->of(
                    [new RawRowValues(['id' => 1, 'undeclared' => 'x'])],
                    schema(int_schema('id')),
                    static fn(mixed $value, Definition $definition): mixed => $value,
                    false,
                )
                ->first()
                ->names(),
        );
    }

    public function test_metadata_is_folded_onto_the_column_with_last_write_winning(): void
    {
        static::assertSame(
            ['k' => 'v2'],
            (new HydratedBatch())
                ->of(
                    [
                        new RawRowValues(['id' => 1], ['id' => Metadata::fromArray(['k' => 'v1'])]),
                        new RawRowValues(['id' => 2], ['id' => Metadata::fromArray(['k' => 'v2'])]),
                    ],
                    schema(int_schema('id')),
                    static fn(mixed $value, Definition $definition): mixed => $value,
                    false,
                )
                ->schema()
                ->get('id')
                ->metadata()
                ->normalize(),
        );
    }

    public function test_metadata_is_folded_onto_a_numeric_column_name(): void
    {
        static::assertSame(
            ['k' => 'v'],
            (new HydratedBatch())
                ->of(
                    [new RawRowValues([], ['0' => Metadata::fromArray(['k' => 'v'])])],
                    schema(int_schema('0', nullable: true)),
                    static fn(mixed $value, Definition $definition): mixed => $value,
                    false,
                )
                ->schema()
                ->get('0')
                ->metadata()
                ->normalize(),
        );
    }

    public function test_metadata_for_an_undeclared_column_is_ignored(): void
    {
        static::assertEquals(
            schema(int_schema('id')),
            (new HydratedBatch())->of(
                [new RawRowValues(['id' => 1], ['nope' => Metadata::fromArray(['k' => 'v'])])],
                schema(int_schema('id')),
                static fn(mixed $value, Definition $definition): mixed => $value,
                false,
            )->schema(),
        );
    }

    public function test_rows_follow_the_schema_column_order(): void
    {
        static::assertSame(
            ['id', 'name'],
            (new HydratedBatch())
                ->of(
                    [new RawRowValues(['name' => 'a', 'id' => 1])],
                    schema(int_schema('id'), str_schema('name')),
                    static fn(mixed $value, Definition $definition): mixed => $value,
                    false,
                )
                ->first()
                ->names(),
        );
    }

    public function test_the_prepare_callback_sees_each_value_with_its_definition(): void
    {
        static::assertEquals(
            rows(schema(int_schema('id')), row(['id' => 2])),
            (new HydratedBatch())->of(
                [new RawRowValues(['id' => 1])],
                schema(int_schema('id')),
                static fn(mixed $value, Definition $definition): mixed => $definition->entry()->name() === 'id'
                    ? 2
                    : $value,
                false,
            ),
        );
    }
}
