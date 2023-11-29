<?php declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\Tests\Benchmark;

use function Flow\ETL\Adapter\CSV\from_csv;
use function Flow\ETL\Adapter\CSV\to_csv;
use Flow\ETL\Config;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
use PhpBench\Attributes\Groups;

#[Groups(['loader'])]
final class CSVLoaderBench
{
    private readonly FlowContext $context;

    private readonly string $outputPath;

    private Rows $rows;

    public function __construct()
    {
        $this->context = new FlowContext(Config::default());
        $this->outputPath = \tempnam(\sys_get_temp_dir(), 'etl_csv_loader_bench') . '.csv';
        $this->rows = new Rows();

        foreach (from_csv(__DIR__ . '/../Fixtures/orders_flow.csv')->extract($this->context) as $rows) {
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
        to_csv($this->outputPath)->load($this->rows, $this->context);
    }
}
