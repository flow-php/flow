<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use DateTimeImmutable;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Transformer\DuplicateRowTransformer;
use Flow\ETL\WithEntry;

use function Flow\ETL\DSL\date_entry;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\string_entry;
use function Flow\ETL\DSL\with_entry;

final class DuplicateRowTransformerTest extends FlowTestCase
{
    public function test_applying_two_transformations(): void
    {
        $rows = rows(
            row(
                int_entry('id', 1),
                string_entry('status', 'active'),
                int_entry('amount', 100),
                date_entry('date_created', '2025-01-01'),
                date_entry('date_deactivated', null),
            ),
            row(
                int_entry('id', 2),
                string_entry('status', 'inactive'),
                int_entry('amount', 100),
                date_entry('date_created', '2025-01-01'),
                date_entry('date_deactivated', '2025-01-03'),
            ),
            row(
                int_entry('id', 3),
                string_entry('status', 'active'),
                int_entry('amount', 100),
                date_entry('date_created', '2025-01-01'),
                date_entry('date_deactivated', null),
            ),
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
                ],
                [
                    'id' => 2,
                    'status' => 'inactive',
                    'amount' => 100,
                    'date_created' => new DateTimeImmutable('2025-01-01'),
                    'date_deactivated' => new DateTimeImmutable('2025-01-03'),
                ],
                [
                    'id' => 3,
                    'status' => 'active',
                    'amount' => 100,
                    'date_created' => new DateTimeImmutable('2025-01-01'),
                    'date_deactivated' => null,
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
            row(int_entry('id', 1), string_entry('status', 'active'), int_entry('amount', 100)),
            row(int_entry('id', 2), string_entry('status', 'active'), int_entry('amount', 100)),
            row(int_entry('id', 3), string_entry('status', 'active'), int_entry('amount', 100)),
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
            row(int_entry('id', 1), string_entry('status', 'active'), int_entry('amount', 100)),
            row(int_entry('id', 2), string_entry('status', 'inactive'), int_entry('amount', 100)),
            row(int_entry('id', 3), string_entry('status', 'active'), int_entry('amount', 100)),
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
}
