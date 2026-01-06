<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Benchmark;

use function Flow\ETL\DSL\ref;
use Flow\ETL\{Row, Rows, Tests\Double\FakeStaticOrdersExtractor};
use PhpBench\Attributes\{BeforeMethods, Groups};

#[BeforeMethods('setUp')]
#[Groups(['building_blocks'])]
final class RowsBench
{
    private Rows $rows;

    private Rows $rows100;

    private Rows $rows1k;

    public function setUp() : void
    {
        $this->rows = (new FakeStaticOrdersExtractor(10_000))->toRows();
        $this->rows1k = (new FakeStaticOrdersExtractor(1_000))->toRows();
        $this->rows100 = (new FakeStaticOrdersExtractor(100))->toRows();
    }

    public function bench_chunk_1_000_on_10k() : void
    {
        foreach ($this->rows->chunks(1_000) as $chunk) {

        }
    }

    public function bench_diff_left_100_on_1k() : void
    {
        $this->rows1k->diffLeft($this->rows100);
    }

    public function bench_diff_right_100_on_1k() : void
    {
        $this->rows1k->diffRight($this->rows100);
    }

    public function bench_drop_100_on_1k() : void
    {
        $this->rows1k->drop(100);
    }

    public function bench_drop_right_10_on_1k() : void
    {
        $this->rows1k->dropRight(100);
    }

    public function bench_entries_on_1k() : void
    {
        foreach ($this->rows1k->entries() as $entries) {

        }
    }

    public function bench_filter_on_1k() : void
    {
        $this->rows1k->filter(fn (Row $row) : bool => $row->valueOf('order_id') === true);
    }

    public function bench_find_on_1k() : void
    {
        $this->rows1k->find(fn (Row $row) : bool => $row->valueOf('order_id') === true);
    }

    public function bench_find_one_on_1k() : void
    {
        $this->rows1k->findOne(fn (Row $row) : bool => $row->valueOf('order_id') === true);
    }

    public function bench_first_on_1k() : void
    {
        $this->rows1k->first();
    }

    public function bench_merge_100_on_1k() : void
    {
        $this->rows1k->merge($this->rows100);
    }

    public function bench_partition_by_on_1k() : void
    {
        $this->rows1k->partitionBy(ref('order_id'));
    }

    public function bench_schema_on_1k_identical_rows() : void
    {
        $this->rows->schema();
    }

    public function bench_sort_asc_on_1k() : void
    {
        $this->rows1k->sortAscending(ref('order_id'));
    }

    public function bench_sort_by_on_1k() : void
    {
        $this->rows1k->sortBy(ref('order_id'));
    }

    public function bench_sort_desc_on_1k() : void
    {
        $this->rows1k->sortDescending(ref('order_id'));
    }

    public function bench_sort_entries_on_1k() : void
    {
        $this->rows1k->sortEntries();
    }

    public function bench_unique_on_1k() : void
    {
        $this->rows1k->unique();
    }
}
