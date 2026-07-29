<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Rows;

/**
 * Opts a WindowFunction into whole-partition evaluation. Ranking derives from a row's position among
 * its peers, which apply() cannot answer without rescanning the partition per row.
 */
interface PartitionRanking
{
    /**
     * One value per partition row, in partition order. The partition is already sorted by the window's
     * ORDER BY.
     *
     * @return list<int>
     */
    public function rankPartition(Rows $partition): array;
}
