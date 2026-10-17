<?php

declare(strict_types=1);

namespace Flow\Parquet\Reader;

use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\Filter\Binding\BoundFilter;
use Flow\Parquet\ParquetFile\Footer;
use Flow\Parquet\Reader\Pruning\PrunedRows;
use Flow\Parquet\Reader\Pruning\RowGroupStatisticsPruner;

final readonly class ReadPlanner
{
    /**
     * The rows to read: from $offset on (raw rows), $limit of them, minus the row groups whose statistics prove no row
     * matches $filter. A filtered plan reads a superset of the matching rows, so the caller evaluates the filter; it
     * refuses a limit, which counts the rows the caller returns.
     */
    public function plan(Footer $footer, ?BoundFilter $filter, int $offset = 0, ?int $limit = null): ReadPlan
    {
        $rows = $footer->rowsNumber();

        if ($filter === null || $limit === 0) {
            return ReadPlan::window($rows, $offset, $limit);
        }

        if ($limit !== null) {
            throw new InvalidArgumentException(
                'A limit counts the rows returned, which a plan filtered by statistics cannot honour',
            );
        }

        $pruned = (new RowGroupStatisticsPruner($footer->rowGroupChunks($filter->columns())))->prune(
            $filter,
            new PrunedRows(RowSelection::window($rows, $offset), RowSelection::none($rows)),
        );

        return new ReadPlan($pruned->selected, $pruned->matched);
    }
}
