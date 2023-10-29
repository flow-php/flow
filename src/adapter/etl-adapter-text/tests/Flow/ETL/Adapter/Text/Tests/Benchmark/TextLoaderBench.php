<?php declare(strict_types=1);

namespace Flow\ETL\Adapter\Text\Tests\Benchmark;

use Flow\ETL\Config;
use Flow\ETL\DSL\Text;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
use PhpBench\Attributes\BeforeMethods;
use PhpBench\Attributes\Groups;
use PhpBench\Attributes\Iterations;
use PhpBench\Attributes\Revs;

#[Iterations(3)]
#[Groups(['loader'])]
final class TextLoaderBench
{
    private ?FlowContext $context = null;

    private ?Rows $rows = null;

    public function setUp() : void
    {
        $this->context = new FlowContext(Config::default());

        $this->rows = new Rows();

        foreach (Text::from(__DIR__ . '/../Fixtures/orders_flow.csv', rows_in_batch: 1)->extract($this->context) as $rows) {
            $this->rows = $this->rows->merge($rows);
        }
    }

    #[BeforeMethods(['setUp'])]
    #[Revs(5)]
    public function bench_load_10k() : void
    {
        Text::to($outputPath = \tempnam(\sys_get_temp_dir(), 'etl_txt_loader_bench') . '.txt')->load($this->rows, $this->context);

        if (!\file_exists($outputPath)) {
            throw new \RuntimeException("Benchmark failed, \"{$outputPath}\" doesn't exist");
        }
        \unlink($outputPath);
    }
}
