<?php declare(strict_types=1);

use Flow\ETL\Rows;
use PhpBench\Attributes\Groups;
use PhpBench\Attributes\Iterations;
use PhpBench\Attributes\Revs;

#[Iterations(5)]
#[Groups(['building_blocks'])]
final class RowsBench
{
    private Rows $rows;

    public function __construct()
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
    }

    #[Revs(5)]
    public function bench_chunk_10() : void
    {
        foreach ($this->rows->chunks(10) as $chunk) {

        }
    }
}
