<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Schema;

use Flow\Benchmarks\BenchmarkRows;
use Flow\Benchmarks\Datasets\Datasets;
use Generator;
use PhpBench\Attributes as Bench;

/**
 * OutputTimeUnit ms because phpbench reports per revolution; inference arm equals footer arm until a sampling source.
 */
final class SchemaBench
{
    public function warm(array $params): void
    {
        Datasets::orders((int) $params['rows'])->floe();
    }

    public function warmRows(array $params): void
    {
        (new RowsSchemaScenario((int) $params['batch']))->warm();
    }

    #[Bench\ParamProviders('rows')]
    #[Bench\Groups(['schema'])]
    #[Bench\BeforeMethods('warm')]
    #[Bench\Revs(1000)]
    #[Bench\OutputTimeUnit('milliseconds', precision: 4)]
    public function bench_schema_inference(array $params): void
    {
        (new SchemaInferenceScenario((int) $params['rows']))->run();
    }

    #[Bench\ParamProviders('rows')]
    #[Bench\Groups(['schema'])]
    #[Bench\BeforeMethods('warm')]
    #[Bench\Revs(1000)]
    #[Bench\OutputTimeUnit('milliseconds', precision: 4)]
    public function bench_schema_from_footer(array $params): void
    {
        (new FooterSchemaScenario((int) $params['rows']))->run();
    }

    #[Bench\ParamProviders(['rows', 'batches'])]
    #[Bench\Groups(['schema'])]
    #[Bench\BeforeMethods('warmRows')]
    public function bench_rows_schema(array $params): void
    {
        (new RowsSchemaScenario((int) $params['batch']))->run();
    }

    public function rows(): Generator
    {
        $rows = BenchmarkRows::count();

        yield number_format($rows) => ['rows' => $rows];
    }

    public function batches(): Generator
    {
        foreach ([1, 100, 1000, 10000] as $batch) {
            yield number_format($batch) => ['batch' => $batch];
        }
    }
}
