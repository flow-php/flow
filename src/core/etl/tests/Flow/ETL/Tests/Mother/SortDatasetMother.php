<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Mother;

use DateTimeImmutable;
use DateTimeZone;
use Flow\ETL\Row\NullsOrder;
use Flow\ETL\Row\Reference;
use Flow\ETL\Schema;

use function array_slice;
use function array_splice;
use function count;
use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function mt_rand;
use function mt_srand;

use const NAN;

/**
 * @type SortDatasetShape = array{
 *     schema: Schema,
 *     references: non-empty-list<Reference>,
 *     runs: list<list<array<string, mixed>>>,
 *     storedBatchSize: int<1, max>,
 *     mergeBatchSize: int<1, max>
 * }
 */
final class SortDatasetMother
{
    /**
     * Rows over small domains (many ties) with nulls, NaN, -0.0, numeric-looking strings and datetimes, split into
     * 2-8 consecutive runs, sorted by 1-3 distinct random columns in random directions and null placements. "id" is unique and never a key.
     *
     * @return SortDatasetShape
     */
    public static function random(int $seed): array
    {
        mt_srand($seed);

        $columns = ['i', 'f', 's', 'at', 'b'];
        $floats = [null, NAN, -0.0, 0.0, 1.5, -2.25, 10.0];
        $strings = [null, '10', '9', '100', 'a', 'B', '', '09'];
        $instants = [
            null,
            '2026-01-01 00:00:00',
            '2026-01-01 00:00:00.5',
            '1969-12-31 23:59:59',
            '2030-06-15 12:00:00',
        ];
        $utc = new DateTimeZone('UTC');
        $rows = [];

        for ($id = 0, $count = mt_rand(0, 60); $id < $count; $id++) {
            $instant = $instants[mt_rand(0, 4)];
            $rows[] = [
                'id' => $id,
                'i' => mt_rand(0, 5) === 0 ? null : mt_rand(-3, 3),
                'f' => $floats[mt_rand(0, 6)],
                's' => $strings[mt_rand(0, 7)],
                'at' => $instant === null ? null : new DateTimeImmutable($instant, $utc),
                'b' => mt_rand(0, 3) === 0 ? null : mt_rand(0, 1) === 1,
            ];
        }

        $references = [];

        for ($k = 0, $keyCount = mt_rand(1, 3); $k < $keyCount; $k++) {
            $reference = ref(array_splice($columns, mt_rand(0, count($columns) - 1), 1)[0]);
            $nulls = mt_rand(0, 1) === 1 ? NullsOrder::FIRST : NullsOrder::LAST;
            $references[] = mt_rand(0, 1) === 1 ? $reference->asc($nulls) : $reference->desc($nulls);
        }

        $runs = [];
        $runCount = mt_rand(2, 8);
        $offset = 0;

        for ($r = 0; $r < $runCount; $r++) {
            $size = $r === ($runCount - 1) ? $count - $offset : mt_rand(0, $count - $offset);
            $runs[] = array_slice($rows, $offset, $size);
            $offset += $size;
        }

        return [
            'schema' => schema(
                int_schema('id'),
                int_schema('i', nullable: true),
                float_schema('f', nullable: true),
                str_schema('s', nullable: true),
                datetime_schema('at', nullable: true),
                bool_schema('b', nullable: true),
            ),
            'references' => $references,
            'runs' => $runs,
            'storedBatchSize' => mt_rand(1, 7),
            'mergeBatchSize' => mt_rand(1, 10),
        ];
    }
}
