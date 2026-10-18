<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Context;

use Flow\Parquet\Filter\Binding\BoundFilter;
use Flow\Parquet\Filter\Filter;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\MetadataRowGroupChunks;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\Reader\Pruning\PageIndexPruner;
use Flow\Parquet\Reader\Pruning\PrunedRows;
use Flow\Parquet\Reader\RowSelection;
use Flow\Parquet\Tests\Double\InMemoryIndexes;
use Flow\Parquet\Tests\Mother\FooterMother;

/**
 * The page index pruning of one row group of 10 rows over INT64 columns `a` and `b`.
 */
final class OneGroupPagePruning
{
    public static function pruned(Filter $filter, InMemoryIndexes $indexes, ?PrunedRows $rows = null): PrunedRows
    {
        $footer = FooterMother::oneGroup(10, FlatColumn::int64('a'), FlatColumn::int64('b'));

        return (new PageIndexPruner(new MetadataRowGroupChunks($footer->metadata()), $indexes))->prune(
            BoundFilter::bind($filter, $footer->schema(), new Options()),
            $rows ?? new PrunedRows(RowSelection::all(10), RowSelection::none(10)),
        );
    }
}
