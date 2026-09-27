<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Format\Floe;

use Flow\Benchmarks\BenchmarkRows;
use Flow\Benchmarks\Datasets\Datasets;
use Generator;
use PhpBench\Attributes as Bench;

#[Bench\BeforeMethods('warm')]
final class FloeReadBench
{
    public function warm(array $params): void
    {
        Datasets::orders((int) $params['rows'])->floe();
    }

    #[Bench\ParamProviders(['rows'])]
    #[Bench\Groups(['format', 'format-floe'])]
    public function bench_floe_read(array $params): void
    {
        (new FloeReadScenario((int) $params['rows']))->run();
    }

    public function rows(): Generator
    {
        $rows = BenchmarkRows::count();

        yield number_format($rows) => ['rows' => $rows];
    }
}
