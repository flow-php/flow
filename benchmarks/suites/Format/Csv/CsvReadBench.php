<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Format\Csv;

use Flow\Benchmarks\BenchmarkRows;
use Flow\Benchmarks\Datasets\Datasets;
use Generator;
use PhpBench\Attributes as Bench;

#[Bench\BeforeMethods('warm')]
final class CsvReadBench
{
    public function warm(array $params): void
    {
        Datasets::orders((int) $params['rows'])->csv();
    }

    #[Bench\ParamProviders('rows')]
    #[Bench\Groups(['format', 'format-csv'])]
    public function bench_csv_read(array $params): void
    {
        (new CsvReadScenario((int) $params['rows']))->run();
    }

    public function rows(): Generator
    {
        $rows = BenchmarkRows::count();

        yield number_format($rows) => ['rows' => $rows];
    }
}
