<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Mother;

use Flow\ETL\Rows;

use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

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
            rows(schema(int_schema('id')), row(['id' => 5]), row(['id' => 4])),
            rows(schema(int_schema('id')), row(['id' => 3]), row(['id' => 2])),
            rows(schema(int_schema('id')), row(['id' => 1]), row(['id' => 0])),
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
            rows(schema(str_schema('g'), int_schema('v')), row(['g' => 'a', 'v' => 1]), row(['g' => 'b', 'v' => 10])),
            rows(schema(str_schema('g'), int_schema('v')), row(['g' => 'a', 'v' => 2]), row(['g' => 'b', 'v' => 20])),
            rows(schema(str_schema('g'), int_schema('v')), row(['g' => 'a', 'v' => 3]), row(['g' => 'b', 'v' => 30])),
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
            rows(schema(str_schema('g'), int_schema('v')), row(['g' => 'a', 'v' => 1]), row(['g' => 'a', 'v' => 2])),
            rows(schema(str_schema('g'), int_schema('v')), row(['g' => 'a', 'v' => 3]), row(['g' => 'b', 'v' => 10])),
            rows(schema(str_schema('g'), int_schema('v')), row(['g' => 'b', 'v' => 20]), row(['g' => 'b', 'v' => 30])),
        ];
    }

    public static function sequentialIds(int $count): Rows
    {
        $sequence = [];

        for ($id = 1; $id <= $count; $id++) {
            $sequence[] = row(['id' => $id]);
        }

        return rows(schema(int_schema('id')), ...$sequence);
    }
}
