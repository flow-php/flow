<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Pipeline;

use Flow\Benchmarks\BenchmarkRows;
use Generator;
use PhpBench\Attributes as Bench;

/**
 * Rung deltas are descriptive only: subtracting two timed runs adds their variances.
 */
#[Bench\BeforeMethods('warm')]
final class PipelineStagesBench
{
    public function warm(array $params): void
    {
        (new SourceFixture(Source::from((string) $params['source']), (int) $params['rows']))->warm();
    }

    #[Bench\ParamProviders(['rows', 'source_csv'])]
    #[Bench\Groups(['pipeline-stages', 'pipeline-csv'])]
    public function bench_stage_read_select_csv(array $params): void
    {
        (new StageScenario(Source::csv, Stage::read_select, (int) $params['rows']))->run();
    }

    #[Bench\ParamProviders(['rows', 'source_csv'])]
    #[Bench\Groups(['pipeline-stages', 'pipeline-csv'])]
    public function bench_stage_read_select_write_csv(array $params): void
    {
        (new StageScenario(Source::csv, Stage::read_select_write, (int) $params['rows']))->run();
    }

    #[Bench\ParamProviders(['rows', 'source_parquet'])]
    #[Bench\Groups(['pipeline-stages', 'pipeline-parquet'])]
    public function bench_stage_read_select_parquet(array $params): void
    {
        (new StageScenario(Source::parquet, Stage::read_select, (int) $params['rows']))->run();
    }

    #[Bench\ParamProviders(['rows', 'source_parquet'])]
    #[Bench\Groups(['pipeline-stages', 'pipeline-parquet'])]
    public function bench_stage_read_select_write_parquet(array $params): void
    {
        (new StageScenario(Source::parquet, Stage::read_select_write, (int) $params['rows']))->run();
    }

    public function rows(): Generator
    {
        $rows = BenchmarkRows::count();

        yield number_format($rows) => ['rows' => $rows];
    }

    public function source_csv(): Generator
    {
        yield 'csv' => ['source' => Source::csv->value];
    }

    public function source_parquet(): Generator
    {
        yield 'parquet' => ['source' => Source::parquet->value];
    }
}
