<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use function Flow\ETL\DSL\{df, from_array, lit, ref, with_entry};
use Flow\ETL\Tests\FlowTestCase;

final class DuplicateRowTest extends FlowTestCase
{
    public function test_duplicating_rows() : void
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

        self::assertCount(4, $rows);
        self::assertEquals(
            [
                ['id' => 1, 'status' => 'active', 'amount' => 100],
                ['id' => 2, 'status' => 'inactive', 'amount' => 100],
                ['id' => 2, 'status' => 'inactive', 'amount' => -100],
                ['id' => 3, 'status' => 'active', 'amount' => 100],
            ],
            $rows
        );
    }

    public function test_duplicating_rows_without_transformations() : void
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

        self::assertCount(4, $rows);
        self::assertEquals(
            [
                ['id' => 1, 'status' => 'active', 'amount' => 100],
                ['id' => 2, 'status' => 'inactive', 'amount' => 100],
                ['id' => 2, 'status' => 'inactive', 'amount' => 100],
                ['id' => 3, 'status' => 'active', 'amount' => 100],
            ],
            $rows
        );
    }
}
