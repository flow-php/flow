<?php declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\Tests\Benchmark;

use Flow\ETL\Config;
use Flow\ETL\DSL\Json;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
use PhpBench\Attributes\Groups;

#[Groups(['loader'])]
final class JsonLoaderBench
{
    private readonly FlowContext $context;

    private readonly string $outputPath;

    private Rows $rows;

    public function __construct()
    {
        $this->context = new FlowContext(Config::default());
        $this->outputPath = \tempnam(\sys_get_temp_dir(), 'etl_json_loader_bench') . '.json';
        $this->rows = new Rows();

        foreach (Json::from(__DIR__ . '/../Fixtures/orders_flow.json')->extract($this->context) as $rows) {
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
        Json::to($this->outputPath)->load($this->rows, $this->context);
    }
}
