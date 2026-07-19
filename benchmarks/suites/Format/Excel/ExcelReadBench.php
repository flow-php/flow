<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Format\Excel;

use Generator;
use PhpBench\Attributes as Bench;

final class ExcelReadBench
{
    #[Bench\ParamProviders('rows')]
    #[Bench\Groups(['format', 'format-excel'])]
    public function bench_excel_read(array $params): void
    {
        (new ExcelReadScenario((int) $params['rows']))->run();
    }

    public function rows(): Generator
    {
        $rows = (int) (getenv('FLOW_BENCH_ROWS') ?: 100_000);

        yield number_format($rows) => ['rows' => $rows];
    }
}
