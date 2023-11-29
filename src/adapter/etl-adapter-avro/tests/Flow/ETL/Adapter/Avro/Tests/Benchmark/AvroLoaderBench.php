<?php declare(strict_types=1);

namespace Flow\ETL\Adapter\Avro\Tests\Benchmark;

use function Flow\ETL\DSL\Adapter\Avro\from_avro;
use function Flow\ETL\DSL\Adapter\Avro\to_avro;
use Flow\ETL\Config;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
use PhpBench\Attributes\Groups;

#[Groups(['loader'])]
final class AvroLoaderBench
{
    private readonly FlowContext $context;

    private readonly string $outputPath;

    private Rows $rows;

    public function __construct()
    {
        $this->context = new FlowContext(Config::default());
        $this->outputPath = \tempnam(\sys_get_temp_dir(), 'etl_avro_loader_bench') . '.avro';
        $this->rows = new Rows();

        foreach (from_avro(__DIR__ . '/../Fixtures/orders_flow.avro')->extract($this->context) as $rows) {
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
        to_avro($this->outputPath)->load($this->rows, $this->context);
    }
}
