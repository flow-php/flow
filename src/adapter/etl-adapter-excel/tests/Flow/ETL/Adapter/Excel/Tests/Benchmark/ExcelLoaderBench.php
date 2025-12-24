<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel\Tests\Benchmark;

use function Flow\ETL\Adapter\Excel\DSL\{from_excel, to_excel};
use function Flow\ETL\DSL\df;
use Flow\ETL\Adapter\Excel\ExcelWriter;
use PhpBench\Attributes\Groups;

#[Groups(['loader'])]
final readonly class ExcelLoaderBench
{
    private string $tempDir;

    public function __construct()
    {
        $this->tempDir = \sys_get_temp_dir() . '/flow_excel_bench_' . \getmypid();

        if (!\is_dir($this->tempDir)) {
            \mkdir($this->tempDir, 0777, true);
        }
    }

    public function bench_load_10k_ods() : void
    {
        $inputPath = __DIR__ . '/../Fixtures/orders_flow.ods';
        $outputPath = $this->tempDir . '/output_bench.ods';

        if (\file_exists($outputPath)) {
            \unlink($outputPath);
        }

        df()
            ->read(from_excel($inputPath))
            ->write(to_excel($outputPath)->withWriter(ExcelWriter::ODS))
            ->run();
    }

    public function bench_load_10k_xlsx() : void
    {
        $inputPath = __DIR__ . '/../Fixtures/orders_flow.xlsx';
        $outputPath = $this->tempDir . '/output_bench.xlsx';

        if (\file_exists($outputPath)) {
            \unlink($outputPath);
        }

        df()
            ->read(from_excel($inputPath))
            ->write(to_excel($outputPath)->withWriter(ExcelWriter::XLSX))
            ->run();
    }
}
