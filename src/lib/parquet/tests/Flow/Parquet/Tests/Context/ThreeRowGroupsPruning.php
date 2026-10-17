<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Context;

use Flow\Parquet\Filter\Binding\BoundFilter;
use Flow\Parquet\Filter\Filter;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\MetadataRowGroupChunks;
use Flow\Parquet\ParquetFile\RowGroupChunks;
use Flow\Parquet\Reader;
use Flow\Parquet\Reader\Pruning\PrunedRows;
use Flow\Parquet\Reader\Pruning\RowGroupStatisticsPruner;
use Flow\Parquet\Reader\RowSelection;

/**
 * The row-group statistics pruning of `MemoryParquetFile::threeRowGroups()`.
 */
final class ThreeRowGroupsPruning
{
    public static function chunks(): MetadataRowGroupChunks
    {
        return new MetadataRowGroupChunks(
            MemoryParquetFile::read(Reader::php(), MemoryParquetFile::threeRowGroups())->metadata(),
        );
    }

    public static function pruned(
        Filter $filter,
        RowSelection $selected,
        ?RowSelection $matched = null,
        ?RowGroupChunks $chunks = null,
    ): PrunedRows {
        $metadata = MemoryParquetFile::read(Reader::php(), MemoryParquetFile::threeRowGroups())->metadata();

        return (new RowGroupStatisticsPruner($chunks ?? new MetadataRowGroupChunks($metadata)))->prune(
            BoundFilter::bind($filter, $metadata->schema(), new Options()),
            new PrunedRows($selected, $matched ?? RowSelection::none(6)),
        );
    }
}
