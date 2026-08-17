<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Context;

use Flow\ETL\Rows;

use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;

/**
 * Five salaries with a three-way tie, pre-sorted. A PartitionRanking function is always handed an
 * already-sorted partition - WindowProcessor sorts before calling it - so the fixtures carry the order
 * rather than relying on the function to impose one.
 *
 * Expectations derived from PostgreSQL.
 */
final class RankingContext
{
    public static function salariesAscending(): Rows
    {
        return rows(
            row(int_entry('id', 4), int_entry('salary', 2000)),
            row(int_entry('id', 5), int_entry('salary', 4000)),
            row(int_entry('id', 1), int_entry('salary', 6000)),
            row(int_entry('id', 2), int_entry('salary', 6000)),
            row(int_entry('id', 3), int_entry('salary', 6000)),
        );
    }

    public static function salariesDescending(): Rows
    {
        return rows(
            row(int_entry('id', 1), int_entry('salary', 6000)),
            row(int_entry('id', 2), int_entry('salary', 6000)),
            row(int_entry('id', 3), int_entry('salary', 6000)),
            row(int_entry('id', 5), int_entry('salary', 4000)),
            row(int_entry('id', 4), int_entry('salary', 2000)),
        );
    }
}
