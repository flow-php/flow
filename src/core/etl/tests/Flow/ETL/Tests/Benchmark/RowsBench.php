<?php declare(strict_types=1);

use function Flow\ETL\DSL\ref;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry\StringEntry;
use Flow\ETL\Rows;
use PhpBench\Attributes\BeforeMethods;
use PhpBench\Attributes\Groups;
use PhpBench\Attributes\Revs;

#[BeforeMethods('setUp')]
#[Revs(2)]
#[Groups(['building_blocks'])]
final class RowsBench
{
    private Rows $reducedRows;

    private Rows $rows;

    public function setUp() : void
    {
        $this->rows = Rows::fromArray(
            \array_merge(...\array_map(static fn () : array => [
                ['id' => 1, 'random' => false, 'text' => null, 'from' => 666],
                ['id' => 2, 'random' => true, 'text' => null, 'from' => 666],
                ['id' => 3, 'random' => false, 'text' => null, 'from' => 666],
                ['id' => 4, 'random' => true, 'text' => null, 'from' => 666],
                ['id' => 5, 'random' => false, 'text' => null, 'from' => 666],
            ], \range(0, 10_000)))
        );

        $this->reducedRows = Rows::fromArray(
            \array_merge(...\array_map(static fn () : array => [
                ['id' => 1, 'random' => false, 'text' => null, 'from' => 666],
                ['id' => 2, 'random' => true, 'text' => null, 'from' => 666],
                ['id' => 3, 'random' => false, 'text' => null, 'from' => 666],
                ['id' => 4, 'random' => true, 'text' => null, 'from' => 666],
                ['id' => 5, 'random' => false, 'text' => null, 'from' => 666],
            ], \range(0, 1000)))
        );
    }

    public function bench_chunk_10_on_10k() : void
    {
        foreach ($this->rows->chunks(10) as $chunk) {

        }
    }

    public function bench_diff_left_1k_on_10k() : void
    {
        $this->rows->diffLeft($this->reducedRows);
    }

    public function bench_diff_right_1k_on_10k() : void
    {
        $this->rows->diffRight($this->reducedRows);
    }

    public function bench_drop_1k_on_10k() : void
    {
        $this->rows->drop(1000);
    }

    public function bench_drop_right_1k_on_10k() : void
    {
        $this->rows->dropRight(1000);
    }

    public function bench_entries_on_10k() : void
    {
        foreach ($this->rows->entries() as $entries) {

        }
    }

    public function bench_filter_on_10k() : void
    {
        $this->rows->filter(fn (Row $row) : bool => $row->valueOf('random') === true);
    }

    public function bench_find_on_10k() : void
    {
        $this->rows->find(fn (Row $row) : bool => $row->valueOf('random') === true);
    }

    #[Revs(10)]
    public function bench_find_one_on_10k() : void
    {
        $this->rows->findOne(fn (Row $row) : bool => $row->valueOf('random') === true);
    }

    #[Revs(10)]
    public function bench_first_on_10k() : void
    {
        $this->rows->first();
    }

    public function bench_flat_map_on_1k() : void
    {
        $this->reducedRows->flatMap(fn (Row $row) : array => [
            $row->add(new StringEntry('name', $row->valueOf('id') . '-name-01')),
            $row->add(new StringEntry('name', $row->valueOf('id') . '-name-02')),
        ]);
    }

    public function bench_map_on_10k() : void
    {
        $this->rows->map(fn (Row $row) : Row => $row->rename('random', 'whatever'));
    }

    public function bench_merge_1k_on_10k() : void
    {
        $this->rows->merge($this->reducedRows);
    }

    public function bench_partition_by_on_10k() : void
    {
        $this->rows->partitionBy(ref('from'));
    }

    public function bench_remove_on_10k() : void
    {
        $this->rows->remove(1001);
    }

    public function bench_sort_asc_on_1k() : void
    {
        $this->reducedRows->sortAscending(ref('random'));
    }

    public function bench_sort_by_on_1k() : void
    {
        $this->reducedRows->sortBy(ref('random'));
    }

    public function bench_sort_desc_on_1k() : void
    {
        $this->reducedRows->sortDescending(ref('random'));
    }

    public function bench_sort_entries_on_1k() : void
    {
        $this->reducedRows->sortEntries();
    }

    public function bench_sort_on_1k() : void
    {
        $this->reducedRows->sort(fn (Row $row, Row $nextRow) : int => $row->valueOf('random') <=> $nextRow->valueOf('random'));
    }

    #[Revs(10)]
    public function bench_take_1k_on_10k() : void
    {
        $this->rows->take(1000);
    }

    #[Revs(10)]
    public function bench_take_right_1k_on_10k() : void
    {
        $this->rows->takeRight(1000);
    }

    public function bench_unique_on_1k() : void
    {
        $this->rows->unique();
    }
}
