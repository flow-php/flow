<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Context;

use Flow\ETL\Rows;

use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
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
        return rows(
            schema(int_schema('id'), int_schema('salary')),
            row(['id' => 4, 'salary' => 2000]),
            row(['id' => 5, 'salary' => 4000]),
            row(['id' => 1, 'salary' => 6000]),
            row(['id' => 2, 'salary' => 6000]),
            row(['id' => 3, 'salary' => 6000]),
        );
    }

    public static function salariesDescending(): Rows
    {
        return rows(
            schema(int_schema('id'), int_schema('salary')),
            row(['id' => 1, 'salary' => 6000]),
            row(['id' => 2, 'salary' => 6000]),
            row(['id' => 3, 'salary' => 6000]),
            row(['id' => 5, 'salary' => 4000]),
            row(['id' => 4, 'salary' => 2000]),
        );
    }
}
