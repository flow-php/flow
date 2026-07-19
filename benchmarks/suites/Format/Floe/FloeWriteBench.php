<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Format\Floe;

use Flow\Floe\FloeEngine;
use Generator;
use PhpBench\Attributes as Bench;

use function extension_loaded;

final class FloeWriteBench
{
    #[Bench\ParamProviders(['rows', 'engines'])]
    #[Bench\Groups(['format', 'format-floe'])]
    public function bench_floe_write(array $params): void
    {
        (new FloeWriteScenario((int) $params['rows'], FloeEngine::from((string) $params['engine'])))->run();
    }

    public function rows(): Generator
    {
        $rows = (int) (getenv('FLOW_BENCH_ROWS') ?: 100_000);

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
