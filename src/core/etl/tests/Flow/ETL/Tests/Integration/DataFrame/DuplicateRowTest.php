<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\with_entry;

final class DuplicateRowTest extends FlowTestCase
{
    public function test_duplicating_rows(): void
    {
        $rows = df()
            ->read(from_array([
                ['id' => 1, 'status' => 'active', 'amount' => 100],
                ['id' => 2, 'status' => 'inactive', 'amount' => 100],
                ['id' => 3, 'status' => 'active', 'amount' => 100],
            ]))
            ->duplicateRow(
                condition: ref('status')->equals('inactive'),
                entries: with_entry('amount', ref('amount')->multiply(lit(-1))),
            )
            ->fetch()
            ->toArray();

        static::assertCount(4, $rows);
        static::assertEquals(
            [
                ['id' => 1, 'status' => 'active', 'amount' => 100],
                ['id' => 2, 'status' => 'inactive', 'amount' => 100],
                ['id' => 2, 'status' => 'inactive', 'amount' => -100],
                ['id' => 3, 'status' => 'active', 'amount' => 100],
            ],
            $rows,
        );
    }

    public function test_duplicating_rows_without_transformations(): void
    {
        $rows = df()
            ->read(from_array([
                ['id' => 1, 'status' => 'active', 'amount' => 100],
                ['id' => 2, 'status' => 'inactive', 'amount' => 100],
                ['id' => 3, 'status' => 'active', 'amount' => 100],
            ]))
            ->duplicateRow(condition: ref('status')->equals('inactive'))
            ->fetch()
            ->toArray();

        static::assertCount(4, $rows);
        static::assertEquals(
            [
                ['id' => 1, 'status' => 'active', 'amount' => 100],
                ['id' => 2, 'status' => 'inactive', 'amount' => 100],
                ['id' => 2, 'status' => 'inactive', 'amount' => 100],
                ['id' => 3, 'status' => 'active', 'amount' => 100],
            ],
            $rows,
        );
    }
}
