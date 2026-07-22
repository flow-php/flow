<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Format\Csv;

use Generator;
use PhpBench\Attributes as Bench;

final class CsvWriteBench
{
    #[Bench\ParamProviders('rows')]
    #[Bench\Groups(['format', 'format-csv'])]
    public function bench_csv_write(array $params): void
    {
        (new CsvWriteScenario((int) $params['rows']))->run();
    }

    public function rows(): Generator
    {
        $rows = (int) (getenv('FLOW_BENCH_ROWS') ?: 100_000);

        yield number_format($rows) => ['rows' => $rows];
    }
}
