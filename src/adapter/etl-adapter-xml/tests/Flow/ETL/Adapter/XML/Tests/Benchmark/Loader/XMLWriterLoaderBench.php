<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML\Tests\Benchmark\Loader;

use function Flow\ETL\Adapter\Xml\from_xml;
use Flow\ETL\Adapter\XML\Loader\XMLWriterLoader;
use Flow\ETL\{Config, FlowContext, Rows};
use Flow\Filesystem\Path;
use PhpBench\Attributes\Groups;

#[Groups(['loader'])]
final class XMLWriterLoaderBench
{
    private readonly FlowContext $context;

    private readonly string $outputPath;

    private Rows $rows;

    public function __construct()
    {
        $this->context = new FlowContext(Config::default());
        $this->outputPath = \tempnam(\sys_get_temp_dir(), 'etl_xml_loader_bench') . '.xml';
        $this->rows = new Rows();

        foreach (from_xml(__DIR__ . '/../Fixtures/flow_orders.xml')->extract($this->context) as $rows) {
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
        $loader = new XMLWriterLoader(Path::realpath($this->outputPath));
        $loader->load($this->rows, $this->context);
    }
}
