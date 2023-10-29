<?php declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Benchmark;

use Flow\ETL\Config;
use Flow\ETL\DSL\Parquet;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
use PhpBench\Attributes\Groups;
use PhpBench\Attributes\Iterations;
use PhpBench\Attributes\Revs;

#[Iterations(3)]
#[Groups(['loader'])]
final class ParquetLoaderBench
{
    private FlowContext $context;

    private Rows $rows;

    public function __construct()
    {
        $this->context = new FlowContext(Config::default());

        $this->rows = new Rows();

        foreach (Parquet::from(__DIR__ . '/../Fixtures/orders_flow.parquet')->extract($this->context) as $rows) {
            $this->rows = $this->rows->merge($rows);
        }
    }

    #[Revs(5)]
    public function bench_load_10k() : void
    {
        Parquet::to($outputPath = \tempnam(\sys_get_temp_dir(), 'etl_parquet_loader_bench') . '.parquet')->load($this->rows, $this->context);

        if (!\file_exists($outputPath)) {
            throw new \RuntimeException("Benchmark failed, \"{$outputPath}\" doesn't exist");
        }
        \unlink($outputPath);
    }
}
