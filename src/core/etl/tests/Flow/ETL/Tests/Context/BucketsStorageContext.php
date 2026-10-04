<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Context;

use Flow\ETL\Rows;
use Generator;

final class BucketsStorageContext
{
    /**
     * Flattens the batches yielded by BucketsStorage::get() into a list of rows.
     *
     * @param Generator<Rows> $batches
     *
     * @return list<array<array-key, mixed>>
     */
    public static function rows(Generator $batches): array
    {
        $rows = [];

        foreach ($batches as $batch) {
            foreach ($batch->toArray() as $row) {
                $rows[] = $row;
            }
        }

        return $rows;
    }
}
