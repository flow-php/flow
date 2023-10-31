<?php declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Benchmark;

use Flow\ETL\Config;
use Flow\ETL\DSL\Entry;
use Flow\ETL\DSL\Parquet;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use PhpBench\Attributes\Groups;

#[Groups(['loader'])]
final class ParquetLoaderBench
{
    private readonly FlowContext $context;

    private readonly string $outputPath;

    private Rows $rows;

    public function __construct()
    {
        $this->context = new FlowContext(Config::default());
        $this->outputPath = \tempnam(\sys_get_temp_dir(), 'etl_parquet_loader_bench') . '.parquet';
        $this->rows = new Rows();

        foreach (Parquet::from(__DIR__ . '/../Fixtures/orders_flow.parquet')->extract($this->context) as $rows) {
            $rows = $rows->map(static function (Row $row) : Row {
                return $row->set(Entry::str('order_id', $row->valueOf('order_id')->toString()));
            });

            $this->rows = $this->rows->merge($rows);
        }
    }

    public function __destruct()
    {
        if (!\file_exists($this->outputPath)) {
            throw new \RuntimeException("Benchmark failed, \"{$this->outputPath}\" doesn't exist");
        }

        \unlink($this->outputPath);
    }

    public function bench_load_10k() : void
    {
        Parquet::to($this->outputPath)->load($this->rows, $this->context);
    }
}
