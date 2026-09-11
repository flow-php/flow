<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Format\Json;

use Flow\Benchmarks\BenchmarkRows;
use Flow\Benchmarks\Datasets\Datasets;
use Generator;
use PhpBench\Attributes as Bench;

#[Bench\BeforeMethods('warm')]
final class JsonReadBench
{
    public function warm(array $params): void
    {
        Datasets::orders((int) $params['rows'])->json();
    }

    #[Bench\ParamProviders('rows')]
    #[Bench\Groups(['format', 'format-json'])]
    public function bench_json_read(array $params): void
    {
        (new JsonReadScenario((int) $params['rows']))->run();
    }

    public function rows(): Generator
    {
        $rows = BenchmarkRows::count();

        yield number_format($rows) => ['rows' => $rows];
    }
}
