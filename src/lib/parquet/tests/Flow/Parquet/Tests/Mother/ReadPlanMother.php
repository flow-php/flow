<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Mother;

use Flow\Parquet\Filter\Binding\BoundFilter;
use Flow\Parquet\Filter\Filter;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\Reader\ReadPlan;
use Flow\Parquet\Reader\RowSelection;
use Flow\Parquet\Tests\Double\InMemoryIndexes;

final class ReadPlanMother
{
    /**
     * The window over a file without page indexes.
     */
    public static function window(int $rows, int $offset = 0, ?int $limit = null): ReadPlan
    {
        return ReadPlan::window($rows, new InMemoryIndexes(), $offset, $limit);
    }

    /**
     * By default every row read and none proven to match, so the row filter of $filter is evaluated on each.
     */
    public static function rowFiltered(
        int $rows,
        Filter $filter,
        Schema $schema,
        ?int $limit = null,
        ?RowSelection $selection = null,
        ?RowSelection $matched = null,
    ): ReadPlan {
        return new ReadPlan(
            $selection ?? RowSelection::all($rows),
            $matched ?? RowSelection::none($rows),
            new InMemoryIndexes(),
            BoundFilter::bind($filter, $schema, new Options())->rowFilter(),
            $limit,
        );
    }
}
