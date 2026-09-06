<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use DateTimeImmutable;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Transformer\DuplicateRowTransformer;
use Flow\ETL\WithEntry;

use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\date_schema;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\string_schema;
use function Flow\ETL\DSL\with_entry;
use function Flow\Types\DSL\type_date;

final class DuplicateRowTransformerTest extends FlowTestCase
{
    public function test_a_transformation_that_adds_a_column_conforms_the_untouched_rows(): void
    {
        $transformed = (new DuplicateRowTransformer(
            ref('status')->equals(lit('inactive')),
            with_entry('flag', lit(true)),
        ))->transform(
            rows(
                schema(int_schema('id'), string_schema('status')),
                row(['id' => 1, 'status' => 'active']),
                row(['id' => 2, 'status' => 'inactive']),
            ),
            flow_context(config()),
        );

        static::assertSame(['id', 'status', 'flag'], $transformed->schema()->references()->names());
        static::assertSame(
            [
                ['id' => 1, 'status' => 'active', 'flag' => null],
                ['id' => 2, 'status' => 'inactive', 'flag' => null],
                ['id' => 2, 'status' => 'inactive', 'flag' => true],
            ],
            $transformed->toArray(),
        );
    }

    public function test_a_transformation_that_narrows_a_column_widens_it_back(): void
    {
        $transformed = (new DuplicateRowTransformer(
            ref('status')->equals(lit('inactive')),
            with_entry('amount', ref('amount')->multiply(lit(-1))),
        ))->transform(
            rows(
                schema(int_schema('id'), string_schema('status'), int_schema('amount', nullable: true)),
                row(['id' => 1, 'status' => 'active', 'amount' => 100]),
                row(['id' => 2, 'status' => 'inactive', 'amount' => 100]),
            ),
            flow_context(config()),
        );

        // Multiply::returns() strips nullability, so the duplicated batch declares a narrower column than
        // the source; the emitted batch must describe both.
        static::assertTrue($transformed->schema()->get('amount')->isNullable());
    }

    public function test_applying_two_transformations(): void
    {
        $rows = rows(
            schema(
                int_schema('id'),
                string_schema('status'),
                int_schema('amount'),
                date_schema('date_created'),
                date_schema('date_deactivated', nullable: true),
                date_schema('date_updated', nullable: true),
            ),
            row([
                'id' => 1,
                'status' => 'active',
                'amount' => 100,
                'date_created' => type_date()->cast('2025-01-01'),
                'date_deactivated' => null,
            ]),
            row([
                'id' => 2,
                'status' => 'inactive',
                'amount' => 100,
                'date_created' => type_date()->cast('2025-01-01'),
                'date_deactivated' => type_date()->cast('2025-01-03'),
            ]),
            row([
                'id' => 3,
                'status' => 'active',
                'amount' => 100,
                'date_created' => type_date()->cast('2025-01-01'),
                'date_deactivated' => null,
            ]),
        );

        $transformedRows = (new DuplicateRowTransformer(
            ref('status')->equals(lit('inactive')),
            with_entry('amount', ref('amount')->multiply(lit(-1))),
            with_entry('date_updated', ref('date_deactivated')),
        ))->transform($rows, flow_context());

        static::assertCount(4, $transformedRows);

        static::assertEquals(
            [
                [
                    'id' => 1,
                    'status' => 'active',
                    'amount' => 100,
                    'date_created' => new DateTimeImmutable('2025-01-01'),
                    'date_deactivated' => null,
                    'date_updated' => null,
                ],
                [
                    'id' => 2,
                    'status' => 'inactive',
                    'amount' => 100,
                    'date_created' => new DateTimeImmutable('2025-01-01'),
                    'date_deactivated' => new DateTimeImmutable('2025-01-03'),
                    'date_updated' => null,
                ],
                [
                    'id' => 3,
                    'status' => 'active',
                    'amount' => 100,
                    'date_created' => new DateTimeImmutable('2025-01-01'),
                    'date_deactivated' => null,
                    'date_updated' => null,
                ],
                [
                    'id' => 2,
                    'status' => 'inactive',
                    'amount' => -100,
                    'date_created' => new DateTimeImmutable('2025-01-01'),
                    'date_deactivated' => new DateTimeImmutable('2025-01-03'),
                    'date_updated' => new DateTimeImmutable('2025-01-03'),
                ],
            ],
            $transformedRows->toArray(),
        );
    }

    public function test_doing_nothing_when_condition_is_not_satisfied(): void
    {
        $rows = rows(
            schema(int_schema('id'), string_schema('status'), int_schema('amount')),
            row(['id' => 1, 'status' => 'active', 'amount' => 100]),
            row(['id' => 2, 'status' => 'active', 'amount' => 100]),
            row(['id' => 3, 'status' => 'active', 'amount' => 100]),
        );

        $transformedRows = (new DuplicateRowTransformer(
            ref('status')->equals(lit('inactive')),
            new WithEntry('amount', ref('amount')->multiply(lit(-1))),
        ))->transform($rows, flow_context());

        static::assertCount(3, $transformedRows);

        static::assertEquals(
            [
                ['id' => 1, 'status' => 'active', 'amount' => 100],
                ['id' => 2, 'status' => 'active', 'amount' => 100],
                ['id' => 3, 'status' => 'active', 'amount' => 100],
            ],
            $transformedRows->toArray(),
        );
    }

    public function test_duplicating_row(): void
    {
        $rows = rows(
            schema(int_schema('id'), string_schema('status'), int_schema('amount')),
            row(['id' => 1, 'status' => 'active', 'amount' => 100]),
            row(['id' => 2, 'status' => 'inactive', 'amount' => 100]),
            row(['id' => 3, 'status' => 'active', 'amount' => 100]),
        );

        $transformedRows = (new DuplicateRowTransformer(
            ref('status')->equals(lit('inactive')),
            new WithEntry('amount', ref('amount')->multiply(lit(-1))),
        ))->transform($rows, flow_context());

        static::assertCount(4, $transformedRows);

        static::assertEquals(
            [
                ['id' => 1, 'status' => 'active', 'amount' => 100],
                ['id' => 2, 'status' => 'inactive', 'amount' => 100],
                ['id' => 3, 'status' => 'active', 'amount' => 100],
                ['id' => 2, 'status' => 'inactive', 'amount' => -100],
            ],
            $transformedRows->toArray(),
        );
    }

    public function test_an_empty_batch_is_returned_without_binding(): void
    {
        $empty = rows(schema());

        static::assertSame($empty, (new DuplicateRowTransformer(ref('missing')->equals(lit(1))))->transform(
            $empty,
            flow_context(),
        ));
    }
}
