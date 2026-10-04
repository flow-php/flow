<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Context;

use Flow\ETL\Rows;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;

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
        return array_to_rows(
            [
                ['id' => 4, 'salary' => 2000],
                ['id' => 5, 'salary' => 4000],
                ['id' => 1, 'salary' => 6000],
                ['id' => 2, 'salary' => 6000],
                ['id' => 3, 'salary' => 6000],
            ],
            schema(int_schema('id'), int_schema('salary')),
        );
    }

    public static function salariesDescending(): Rows
    {
        return array_to_rows(
            [
                ['id' => 1, 'salary' => 6000],
                ['id' => 2, 'salary' => 6000],
                ['id' => 3, 'salary' => 6000],
                ['id' => 5, 'salary' => 4000],
                ['id' => 4, 'salary' => 2000],
            ],
            schema(int_schema('id'), int_schema('salary')),
        );
    }
}
