<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row;

use Flow\ETL\Row\HydratedBatch;
use Flow\ETL\Row\RawRowValues;
use Flow\ETL\Schema\Definition;
use Flow\ETL\Schema\Metadata;
use Flow\ETL\Tests\FlowTestCase;

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
