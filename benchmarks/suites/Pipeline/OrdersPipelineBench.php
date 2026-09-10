<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Pipeline;

use Flow\Benchmarks\BenchmarkRows;
use Generator;
use PhpBench\Attributes as Bench;

/**
 * Rung 4 of the ladder: read -> withEntry -> filter -> select -> write, one subject per source.
 *
 * Parquet, Floe, Doctrine and PostgreSQL have no inferred arm on purpose. They declare from metadata
 * (footer / driver describe-query) and never sample, so the inferred-vs-declared comparison is
 * meaningless for them - that is the finding, not an omission.
 */
#[Bench\BeforeMethods('warm')]
final class OrdersPipelineBench
{
    public function warm(array $params): void
    {
        (new SourceFixture(Source::from((string) $params['source']), (int) $params['rows']))->warm();
    }

    #[Bench\ParamProviders(['rows', 'source_csv'])]
    #[Bench\Groups(['pipeline', 'pipeline-csv'])]
    public function bench_pipeline_csv_declared(array $params): void
    {
        (new PipelineScenario(Source::csv, SchemaMode::declared, (int) $params['rows']))->run();
    }

    #[Bench\ParamProviders(['rows', 'source_json'])]
    #[Bench\Groups(['pipeline', 'pipeline-json'])]
    public function bench_pipeline_json_declared(array $params): void
    {
        (new PipelineScenario(Source::json, SchemaMode::declared, (int) $params['rows']))->run();
    }

    #[Bench\ParamProviders(['rows', 'source_json_lines'])]
    #[Bench\Groups(['pipeline', 'pipeline-json-lines'])]
    public function bench_pipeline_json_lines_declared(array $params): void
    {
        (new PipelineScenario(Source::json_lines, SchemaMode::declared, (int) $params['rows']))->run();
    }

    #[Bench\ParamProviders(['rows', 'source_excel'])]
    #[Bench\Groups(['pipeline', 'pipeline-excel'])]
    public function bench_pipeline_excel_declared(array $params): void
    {
        (new PipelineScenario(Source::excel, SchemaMode::declared, (int) $params['rows']))->run();
    }

    #[Bench\ParamProviders(['rows', 'source_parquet'])]
    #[Bench\Groups(['pipeline', 'pipeline-parquet'])]
    public function bench_pipeline_parquet_declared(array $params): void
    {
        (new PipelineScenario(Source::parquet, SchemaMode::declared, (int) $params['rows']))->run();
    }

    #[Bench\ParamProviders(['rows', 'source_floe'])]
    #[Bench\Groups(['pipeline', 'pipeline-floe'])]
    public function bench_pipeline_floe_declared(array $params): void
    {
        (new PipelineScenario(Source::floe, SchemaMode::declared, (int) $params['rows']))->run();
    }

    #[Bench\ParamProviders(['rows', 'source_array'])]
    #[Bench\Groups(['pipeline', 'pipeline-array'])]
    public function bench_pipeline_array_declared(array $params): void
    {
        (new PipelineScenario(Source::array, SchemaMode::declared, (int) $params['rows']))->run();
    }

    #[Bench\ParamProviders(['rows', 'source_memory'])]
    #[Bench\Groups(['pipeline', 'pipeline-memory'])]
    public function bench_pipeline_memory_declared(array $params): void
    {
        (new PipelineScenario(Source::memory, SchemaMode::declared, (int) $params['rows']))->run();
    }

    #[Bench\ParamProviders(['rows', 'source_csv'])]
    #[Bench\Groups(['pipeline', 'pipeline-csv'])]
    public function bench_pipeline_csv_inferred(array $params): void
    {
        (new PipelineScenario(Source::csv, SchemaMode::inferred, (int) $params['rows']))->run();
    }

    #[Bench\ParamProviders(['rows', 'source_json'])]
    #[Bench\Groups(['pipeline', 'pipeline-json'])]
    public function bench_pipeline_json_inferred(array $params): void
    {
        (new PipelineScenario(Source::json, SchemaMode::inferred, (int) $params['rows']))->run();
    }

    #[Bench\ParamProviders(['rows', 'source_json_lines'])]
    #[Bench\Groups(['pipeline', 'pipeline-json-lines'])]
    public function bench_pipeline_json_lines_inferred(array $params): void
    {
        (new PipelineScenario(Source::json_lines, SchemaMode::inferred, (int) $params['rows']))->run();
    }

    #[Bench\ParamProviders(['rows', 'source_excel'])]
    #[Bench\Groups(['pipeline', 'pipeline-excel'])]
    public function bench_pipeline_excel_inferred(array $params): void
    {
        (new PipelineScenario(Source::excel, SchemaMode::inferred, (int) $params['rows']))->run();
    }

    #[Bench\ParamProviders(['rows', 'source_array'])]
    #[Bench\Groups(['pipeline', 'pipeline-array'])]
    public function bench_pipeline_array_inferred(array $params): void
    {
        (new PipelineScenario(Source::array, SchemaMode::inferred, (int) $params['rows']))->run();
    }

    #[Bench\ParamProviders(['rows', 'source_memory'])]
    #[Bench\Groups(['pipeline', 'pipeline-memory'])]
    public function bench_pipeline_memory_inferred(array $params): void
    {
        (new PipelineScenario(Source::memory, SchemaMode::inferred, (int) $params['rows']))->run();
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
