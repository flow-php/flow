<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Format\Floe;

use Flow\Benchmarks\BenchmarkRows;
use Flow\Benchmarks\Datasets\Datasets;
use Flow\Floe\FloeEngine;
use Generator;
use PhpBench\Attributes as Bench;

use function extension_loaded;

#[Bench\BeforeMethods('warm')]
final class FloeWriteBench
{
    public function warm(array $params): void
    {
        Datasets::orders((int) $params['rows'])->floe();
    }

    #[Bench\ParamProviders(['rows', 'engines'])]
    #[Bench\Groups(['format', 'format-floe'])]
    public function bench_floe_write(array $params): void
    {
        (new FloeWriteScenario((int) $params['rows'], FloeEngine::from((string) $params['engine'])))->run();
    }

    public function rows(): Generator
    {
        $rows = BenchmarkRows::count();

        yield number_format($rows) => ['rows' => $rows];
    }

    public function engines(): Generator
    {
        yield 'php' => ['engine' => FloeEngine::php->value];

        if (extension_loaded('flow_php')) {
            yield 'native' => ['engine' => FloeEngine::native->value];
        }
    }
}
