<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use function Flow\ETL\DSL\{date_entry, flow_context, int_entry, lit, ref, row, rows, string_entry, with_entry};
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Transformer\DuplicateRowTransformer;
use Flow\ETL\WithEntry;

final class DuplicateRowTransformerTest extends FlowTestCase
{
    public function test_applying_two_transformations() : void
    {
        $rows = rows(
            row(int_entry('id', 1), string_entry('status', 'active'), int_entry('amount', 100), date_entry('date_created', '2025-01-01'), date_entry('date_deactivated', null)),
            row(int_entry('id', 2), string_entry('status', 'inactive'), int_entry('amount', 100), date_entry('date_created', '2025-01-01'), date_entry('date_deactivated', '2025-01-03')),
            row(int_entry('id', 3), string_entry('status', 'active'), int_entry('amount', 100), date_entry('date_created', '2025-01-01'), date_entry('date_deactivated', null)),
        );

        $transformedRows = (new DuplicateRowTransformer(
            ref('status')->equals(lit('inactive')),
            with_entry('amount', ref('amount')->multiply(lit(-1))),
            with_entry('date_updated', ref('date_deactivated')),
        ))->transform($rows, flow_context());

        self::assertCount(4, $transformedRows);

        self::assertEquals(
            [
                ['id' => 1, 'status' => 'active', 'amount' => 100, 'date_created' => new \DateTimeImmutable('2025-01-01'), 'date_deactivated' => null],
                ['id' => 2, 'status' => 'inactive', 'amount' => 100, 'date_created' => new \DateTimeImmutable('2025-01-01'), 'date_deactivated' => new \DateTimeImmutable('2025-01-03')],
                ['id' => 3, 'status' => 'active', 'amount' => 100, 'date_created' => new \DateTimeImmutable('2025-01-01'), 'date_deactivated' => null],
                ['id' => 2, 'status' => 'inactive', 'amount' => -100, 'date_created' => new \DateTimeImmutable('2025-01-01'), 'date_deactivated' => new \DateTimeImmutable('2025-01-03'), 'date_updated' => new \DateTimeImmutable('2025-01-03')],
            ],
            $transformedRows->toArray()
        );
    }

    public function test_doing_nothing_when_condition_is_not_satisfied() : void
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

        self::assertCount(3, $transformedRows);

        self::assertEquals(
            [
                ['id' => 1, 'status' => 'active', 'amount' => 100],
                ['id' => 2, 'status' => 'active', 'amount' => 100],
                ['id' => 3, 'status' => 'active', 'amount' => 100],
            ],
            $transformedRows->toArray()
        );
    }

    public function test_duplicating_row() : void
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

        self::assertCount(4, $transformedRows);

        self::assertEquals(
            [
                ['id' => 1, 'status' => 'active', 'amount' => 100],
                ['id' => 2, 'status' => 'inactive', 'amount' => 100],
                ['id' => 3, 'status' => 'active', 'amount' => 100],
                ['id' => 2, 'status' => 'inactive', 'amount' => -100],
            ],
            $transformedRows->toArray()
        );
    }
}
