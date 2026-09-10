<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Format\Excel;

use Flow\Benchmarks\BenchmarkRows;
use Flow\Benchmarks\Datasets\Datasets;
use Generator;
use PhpBench\Attributes as Bench;

#[Bench\BeforeMethods('warm')]
final class ExcelReadBench
{
    public function warm(array $params): void
    {
        Datasets::orders((int) $params['rows'])->excel();
    }

    #[Bench\ParamProviders('rows')]
    #[Bench\Groups(['format', 'format-excel'])]
    public function bench_excel_read(array $params): void
    {
        (new ExcelReadScenario((int) $params['rows']))->run();
    }

    public function rows(): Generator
    {
        $rows = BenchmarkRows::count();

        yield number_format($rows) => ['rows' => $rows];
    }
}
