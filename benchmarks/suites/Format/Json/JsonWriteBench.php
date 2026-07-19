<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Format\Json;

use Generator;
use PhpBench\Attributes as Bench;

final class JsonWriteBench
{
    #[Bench\ParamProviders('rows')]
    #[Bench\Groups(['format', 'format-json'])]
    public function bench_json_write(array $params): void
    {
        (new JsonWriteScenario((int) $params['rows']))->run();
    }

    public function rows(): Generator
    {
        $rows = (int) (getenv('FLOW_BENCH_ROWS') ?: 100_000);

        yield number_format($rows) => ['rows' => $rows];
    }
}
