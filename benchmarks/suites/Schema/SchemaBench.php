<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Schema;

use Generator;
use PhpBench\Attributes as Bench;

final class SchemaBench
{
    #[Bench\ParamProviders('rows')]
    #[Bench\Groups(['schema'])]
    public function bench_schema_inference(array $params): void
    {
        (new SchemaInferenceScenario((int) $params['rows']))->run();
    }

    public function rows(): Generator
    {
        $rows = (int) (getenv('FLOW_BENCH_ROWS') ?: 100_000);

        yield number_format($rows) => ['rows' => $rows];
    }
}
