<?php declare(strict_types=1);

namespace Flow\ETL\Adapter\Text\Tests\Benchmark;

use function Flow\ETL\Adapter\Text\from_text;
use function Flow\ETL\Adapter\Text\to_text;
use Flow\ETL\Config;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
use PhpBench\Attributes\Groups;

#[Groups(['loader'])]
final class TextLoaderBench
{
    private readonly FlowContext $context;

    private readonly string $outputPath;

    private Rows $rows;

    public function __construct()
    {
        $this->context = new FlowContext(Config::default());
        $this->outputPath = \tempnam(\sys_get_temp_dir(), 'etl_txt_loader_bench') . '.txt';
        $this->rows = new Rows();

        foreach (from_text(__DIR__ . '/../Fixtures/orders_flow.csv')->extract($this->context) as $rows) {
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
        to_text($this->outputPath)->load($this->rows, $this->context);
    }
}
