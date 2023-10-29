<?php declare(strict_types=1);

namespace Flow\ETL\Adapter\Avro\Tests\Benchmark;

use Flow\ETL\Config;
use Flow\ETL\DSL\Avro;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
use PhpBench\Attributes\BeforeMethods;
use PhpBench\Attributes\Groups;
use PhpBench\Attributes\Iterations;
use PhpBench\Attributes\Revs;

#[Iterations(3)]
#[Groups(['loader'])]
final class AvroLoaderBench
{
    private ?FlowContext $context = null;

    private ?Rows $rows = null;

    public function setUp() : void
    {
        $this->context = new FlowContext(Config::default());

        $this->rows = new Rows();

        foreach (Avro::from(__DIR__ . '/../Fixtures/orders_flow.avro')->extract($this->context) as $rows) {
            $this->rows = $this->rows->merge($rows);
        }
    }

    #[BeforeMethods(['setUp'])]
    #[Revs(5)]
    public function bench_load_10k() : void
    {
        Avro::to($outputPath = \tempnam(\sys_get_temp_dir(), 'etl_avro_loader_bench') . '.avro')->load($this->rows, $this->context);

        if (!\file_exists($outputPath)) {
            throw new \RuntimeException("Benchmark failed, \"{$outputPath}\" doesn't exist");
        }
        \unlink($outputPath);
    }
}
