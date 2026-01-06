<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel\Tests\Benchmark;

use function Flow\ETL\Adapter\Excel\DSL\to_excel;
use function Flow\ETL\DSL\flow_context;
use Flow\ETL\Adapter\Excel\ExcelWriter;
use Flow\ETL\{FlowContext, Rows};
use Flow\ETL\Tests\Double\FakeStaticOrdersExtractor;
use PhpBench\Attributes\Groups;

#[Groups(['loader'])]
final readonly class ExcelLoaderBench
{
    private FlowContext $context;

    private Rows $rows;

    private string $tempDir;

    public function __construct()
    {
        $this->tempDir = \sys_get_temp_dir() . '/flow_excel_bench_' . \getmypid();

        if (!\is_dir($this->tempDir)) {
            \mkdir($this->tempDir, 0777, true);
        }

        $this->rows = (new FakeStaticOrdersExtractor(10_000))->toRows();
        $this->context = flow_context();
    }

    public function bench_load_10k_ods() : void
    {
        $outputPath = $this->tempDir . '/output_bench.ods';

        to_excel($outputPath)->withWriter(ExcelWriter::ODS)->load($this->rows, $this->context);
    }

    public function bench_load_10k_xlsx() : void
    {
        $outputPath = $this->tempDir . '/output_bench.xlsx';

        if (\file_exists($outputPath)) {
            \unlink($outputPath);
        }

        to_excel($outputPath)->withWriter(ExcelWriter::XLSX)->load($this->rows, $this->context);
    }
}
