<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Benchmark;

use function Flow\ETL\DSL\ref;
use Flow\ETL\{Row, Rows, Tests\Double\FakeStaticOrdersExtractor};
use PhpBench\Attributes\{BeforeMethods, Groups, Iterations, Revs};

#[BeforeMethods('setUp')]
#[Groups(['data-frame'])]
#[Revs(100)]
#[Iterations(10)]
final class RowsBench
{
    private Rows $rows;

    private Rows $rows10;

    private Rows $rows100;

    public function setUp() : void
    {
        $this->rows = (new FakeStaticOrdersExtractor(1_000))->toRows();
        $this->rows100 = (new FakeStaticOrdersExtractor(100))->toRows();
        $this->rows10 = (new FakeStaticOrdersExtractor(10))->toRows();
    }

    #[Revs(1000)]
    public function bench_chunk_100_on_1k() : void
    {
        foreach ($this->rows->chunks(100) as $chunk) {

        }
    }

    public function bench_diff_left_10_on_100() : void
    {
        $this->rows100->diffLeft($this->rows10);
    }

    public function bench_diff_right_10_on_100() : void
    {
        $this->rows100->diffRight($this->rows10);
    }

    public function bench_filter_on_100() : void
    {
        $this->rows100->filter(fn (Row $row) : bool => $row->valueOf('order_id') === true);
    }

    public function bench_find_on_100() : void
    {
        $this->rows100->find(fn (Row $row) : bool => $row->valueOf('order_id') === true);
    }

    public function bench_find_one_on_100() : void
    {
        $this->rows100->findOne(fn (Row $row) : bool => $row->valueOf('order_id') === true);
    }

    public function bench_partition_by_on_100() : void
    {
        $this->rows100->partitionBy(ref('order_id'));
    }

    public function bench_sort_asc_on_100() : void
    {
        $this->rows100->sortAscending(ref('order_id'));
    }

    public function bench_sort_by_on_100() : void
    {
        $this->rows100->sortBy(ref('order_id'));
    }

    public function bench_sort_desc_on_100() : void
    {
        $this->rows100->sortDescending(ref('order_id'));
    }

    public function bench_sort_entries_on_100() : void
    {
        $this->rows100->sortEntries();
    }

    public function bench_unique_on_100() : void
    {
        $this->rows100->unique();
    }
}
