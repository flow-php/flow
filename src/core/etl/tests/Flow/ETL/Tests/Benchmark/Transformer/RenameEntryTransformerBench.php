<?php declare(strict_types=1);

namespace Flow\ETL\Tests\Benchmark\Transformer;

use Flow\ETL\Config;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
use Flow\ETL\Transformer\RenameEntryTransformer;
use PhpBench\Attributes\Groups;
use PhpBench\Attributes\Iterations;
use PhpBench\Attributes\Revs;

#[Iterations(5)]
#[Groups(['transformer'])]
final class RenameEntryTransformerBench
{
    private FlowContext $context;

    private Rows $rows;

    public function __construct()
    {
        $this->rows = Rows::fromArray(
            \array_merge(...\array_map(static function () : array {
                return [
                    ['id' => 1, 'random' => false, 'text' => null, 'from' => 666],
                    ['id' => 2, 'random' => true, 'text' => null, 'from' => 666],
                    ['id' => 3, 'random' => false, 'text' => null, 'from' => 666],
                    ['id' => 4, 'random' => true, 'text' => null, 'from' => 666],
                    ['id' => 5, 'random' => false, 'text' => null, 'from' => 666],
                ];
            }, \range(0, 10_000)))
        );
        $this->context = new FlowContext(Config::default());
    }

    #[Revs(5)]
    public function bench_transform_10k_rows() : void
    {
        (new RenameEntryTransformer('from', 'to'))->transform($this->rows, $this->context);
    }
}
