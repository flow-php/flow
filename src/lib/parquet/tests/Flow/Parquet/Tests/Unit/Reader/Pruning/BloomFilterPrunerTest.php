<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Reader\Pruning;

use Flow\Parquet\Filter\Binding\BoundFilter;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\MetadataRowGroupChunks;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\Reader\Pruning\BloomFilterPruner;
use Flow\Parquet\Reader\Pruning\PrunedRows;
use Flow\Parquet\Reader\RowSelection;
use Flow\Parquet\Reader\RowSelector;
use Flow\Parquet\Tests\Double\InMemoryIndexes;
use Flow\Parquet\Tests\Mother\FooterMother;
use Flow\Parquet\Tests\Mother\SbbfMother;
use PHPUnit\Framework\TestCase;

use function Flow\Parquet\DSL\parquet_column;

final class BloomFilterPrunerTest extends TestCase
{
    public function test_a_group_whose_filter_lacks_the_literal_is_dropped_and_matched_is_kept(): void
    {
        $footer = FooterMother::idGroups([0, 10], [0, 10], [0, 10]);
        $id = FlatColumn::int64('id');
        $indexes = new InMemoryIndexes(bloomFilters: ['id' => [
            SbbfMother::with($id, 0, 10),
            SbbfMother::with($id, 4, 10),
            SbbfMother::with($id, 0, 10),
        ]]);
        $matched = new RowSelection(new RowSelector(2, true), new RowSelector(2, false), new RowSelector(2, true));

        static::assertEquals(
            new PrunedRows(
                new RowSelection(new RowSelector(2, true), new RowSelector(2, false), new RowSelector(2, true)),
                $matched,
            ),
            (new BloomFilterPruner(new MetadataRowGroupChunks($footer->metadata()), $indexes))->prune(
                BoundFilter::bind(parquet_column('id')->equals(4), $footer->schema(), new Options()),
                new PrunedRows(RowSelection::all(6), $matched),
            ),
        );
    }

    public function test_no_filter_is_read_for_a_range_predicate(): void
    {
        $footer = FooterMother::idGroups([0, 10], [0, 10]);
        $indexes = new InMemoryIndexes(bloomFilters: ['id' => [
            SbbfMother::with(FlatColumn::int64('id'), 0),
            SbbfMother::with(FlatColumn::int64('id'), 0),
        ]]);
        $rows = new PrunedRows(RowSelection::all(4), RowSelection::none(4));

        static::assertEquals($rows, (new BloomFilterPruner(
            new MetadataRowGroupChunks($footer->metadata()),
            $indexes,
        ))->prune(BoundFilter::bind(parquet_column('id')->greaterThan(4), $footer->schema(), new Options()), $rows));
        static::assertSame([], $indexes->reads);
    }

    public function test_a_group_selecting_no_row_or_matched_already_reads_no_filter(): void
    {
        $footer = FooterMother::idGroups([0, 10], [0, 10], [0, 10]);
        $indexes = new InMemoryIndexes(bloomFilters: ['id' => [
            SbbfMother::with(FlatColumn::int64('id'), 0),
            SbbfMother::with(FlatColumn::int64('id'), 0),
            SbbfMother::with(FlatColumn::int64('id'), 0),
        ]]);
        $selected = new RowSelection(new RowSelector(2, true), new RowSelector(4, false));
        $matched = new RowSelection(new RowSelector(2, true), new RowSelector(2, false), new RowSelector(2, true));

        static::assertEquals(
            new PrunedRows($matched, $matched),
            (new BloomFilterPruner(new MetadataRowGroupChunks($footer->metadata()), $indexes))->prune(
                BoundFilter::bind(parquet_column('id')->equals(4), $footer->schema(), new Options()),
                new PrunedRows($selected, $matched),
            ),
        );
        static::assertSame([['bloomFilter', 'id', 2]], $indexes->reads);
    }
}
