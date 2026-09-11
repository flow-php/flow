<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Pipeline;

use Flow\Benchmarks\BenchmarkRows;
use Generator;
use PhpBench\Attributes as Bench;

/**
 * Rung 1, per source. Without a read-only rung per source, a read regression lands inside the
 * aggregate pipeline's noise band.
 */
#[Bench\BeforeMethods('warm')]
final class PipelineReadBench
{
    public function warm(array $params): void
    {
        (new SourceFixture(Source::from((string) $params['source']), (int) $params['rows']))->warm();
    }

    #[Bench\ParamProviders(['rows', 'source_csv'])]
    #[Bench\Groups(['pipeline-read', 'pipeline-csv'])]
    public function bench_read_csv(array $params): void
    {
        (new ReadScenario(Source::csv, (int) $params['rows']))->run();
    }

    #[Bench\ParamProviders(['rows', 'source_json'])]
    #[Bench\Groups(['pipeline-read', 'pipeline-json'])]
    public function bench_read_json(array $params): void
    {
        (new ReadScenario(Source::json, (int) $params['rows']))->run();
    }

    #[Bench\ParamProviders(['rows', 'source_json_lines'])]
    #[Bench\Groups(['pipeline-read', 'pipeline-json-lines'])]
    public function bench_read_json_lines(array $params): void
    {
        (new ReadScenario(Source::json_lines, (int) $params['rows']))->run();
    }

    #[Bench\ParamProviders(['rows', 'source_excel'])]
    #[Bench\Groups(['pipeline-read', 'pipeline-excel'])]
    public function bench_read_excel(array $params): void
    {
        (new ReadScenario(Source::excel, (int) $params['rows']))->run();
    }

    #[Bench\ParamProviders(['rows', 'source_parquet'])]
    #[Bench\Groups(['pipeline-read', 'pipeline-parquet'])]
    public function bench_read_parquet(array $params): void
    {
        (new ReadScenario(Source::parquet, (int) $params['rows']))->run();
    }

    #[Bench\ParamProviders(['rows', 'source_floe'])]
    #[Bench\Groups(['pipeline-read', 'pipeline-floe'])]
    public function bench_read_floe(array $params): void
    {
        (new ReadScenario(Source::floe, (int) $params['rows']))->run();
    }

    #[Bench\ParamProviders(['rows', 'source_array'])]
    #[Bench\Groups(['pipeline-read', 'pipeline-array'])]
    public function bench_read_array(array $params): void
    {
        (new ReadScenario(Source::array, (int) $params['rows']))->run();
    }

    #[Bench\ParamProviders(['rows', 'source_memory'])]
    #[Bench\Groups(['pipeline-read', 'pipeline-memory'])]
    public function bench_read_memory(array $params): void
    {
        (new ReadScenario(Source::memory, (int) $params['rows']))->run();
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

    public function source_json(): Generator
    {
        yield 'json' => ['source' => Source::json->value];
    }

    public function source_json_lines(): Generator
    {
        yield 'json_lines' => ['source' => Source::json_lines->value];
    }

    public function source_excel(): Generator
    {
        yield 'excel' => ['source' => Source::excel->value];
    }

    public function source_parquet(): Generator
    {
        yield 'parquet' => ['source' => Source::parquet->value];
    }

    public function source_floe(): Generator
    {
        yield 'floe' => ['source' => Source::floe->value];
    }

    public function source_array(): Generator
    {
        yield 'array' => ['source' => Source::array->value];
    }

    public function source_memory(): Generator
    {
        yield 'memory' => ['source' => Source::memory->value];
    }
}
