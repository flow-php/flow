<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Mother;

use Flow\ETL\Rows;

use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\str_entry;

final class RowsMother
{
    /**
     * 3 batches of 2 rows, ids descending: [5,4][3,2][1,0].
     *
     * @return list<Rows>
     */
    public static function descendingIdBatches(): array
    {
        return [
            rows(row(int_entry('id', 5)), row(int_entry('id', 4))),
            rows(row(int_entry('id', 3)), row(int_entry('id', 2))),
            rows(row(int_entry('id', 1)), row(int_entry('id', 0))),
        ];
    }

    /**
     * 3 batches of 2 rows, groups interleaved: [a1,b10][a2,b20][a3,b30].
     *
     * @return list<Rows>
     */
    public static function interleavedGroupBatches(): array
    {
        return [
            rows(row(str_entry('g', 'a'), int_entry('v', 1)), row(str_entry('g', 'b'), int_entry('v', 10))),
            rows(row(str_entry('g', 'a'), int_entry('v', 2)), row(str_entry('g', 'b'), int_entry('v', 20))),
            rows(row(str_entry('g', 'a'), int_entry('v', 3)), row(str_entry('g', 'b'), int_entry('v', 30))),
        ];
    }

    /**
     * 3 batches of 2 rows, groups already consecutive but straddling batch boundaries:
     * [a1,a2][a3,b10][b20,b30].
     *
     * @return list<Rows>
     */
    public static function sortedGroupBatches(): array
    {
        return [
            rows(row(str_entry('g', 'a'), int_entry('v', 1)), row(str_entry('g', 'a'), int_entry('v', 2))),
            rows(row(str_entry('g', 'a'), int_entry('v', 3)), row(str_entry('g', 'b'), int_entry('v', 10))),
            rows(row(str_entry('g', 'b'), int_entry('v', 20)), row(str_entry('g', 'b'), int_entry('v', 30))),
        ];
    }

    public static function sequentialIds(int $count): Rows
    {
        $sequence = [];

        for ($id = 1; $id <= $count; $id++) {
            $sequence[] = row(int_entry('id', $id));
        }

        return rows(...$sequence);
    }
}
