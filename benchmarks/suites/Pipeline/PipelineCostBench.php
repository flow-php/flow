<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Pipeline;

use Flow\Benchmarks\BenchmarkRows;
use Generator;
use PhpBench\Attributes as Bench;

/**
 * Per-cost subjects. Every one reads CSV with a declared schema so the read leg is constant and the
 * subject prices only the thing it names.
 */
#[Bench\BeforeMethods('warm')]
final class PipelineCostBench
{
    public function warm(array $params): void
    {
        (new SourceFixture(Source::csv, (int) $params['rows']))->warm();
    }

    #[Bench\ParamProviders(['rows', 'mixes'])]
    #[Bench\Groups(['pipeline-cost'])]
    public function bench_pipeline_column_mix(array $params): void
    {
        (new ColumnMixScenario(Source::csv, ColumnMix::from((string) $params['mix']), (int) $params['rows']))->run();
    }

    #[Bench\ParamProviders(['rows', 'widths'])]
    #[Bench\Groups(['pipeline-cost'])]
    public function bench_pipeline_width(array $params): void
    {
        (new PipelineWidthScenario(Source::csv, (int) $params['width'], (int) $params['rows']))->run();
    }

    #[Bench\ParamProviders(['rows', 'depths'])]
    #[Bench\Groups(['pipeline-cost'])]
    #[Bench\Revs(10)]
    public function bench_plan_bind_depth(array $params): void
    {
        (new PlanDepthScenario(Source::csv, (int) $params['depth'], (int) $params['rows']))->run();
    }

    #[Bench\ParamProviders('rows')]
    #[Bench\Groups(['pipeline-cost'])]
    public function bench_fetch_materialisation(array $params): void
    {
        (new FetchScenario(Source::csv, (int) $params['rows']))->run();
    }

    #[Bench\ParamProviders(['rows', 'limits'])]
    #[Bench\Groups(['pipeline-cost'])]
    public function bench_limit_pushdown(array $params): void
    {
        $limit = $params['limit'] === null ? null : (int) $params['limit'];

        (new LimitPushdownScenario(Source::csv, $limit, (int) $params['rows']))->run();
    }

    public function rows(): Generator
    {
        $rows = BenchmarkRows::count();

        yield number_format($rows) => ['rows' => $rows];
    }

    public function mixes(): Generator
    {
        foreach (ColumnMix::cases() as $mix) {
            yield $mix->value => ['mix' => $mix->value];
        }
    }

    public function widths(): Generator
    {
        foreach ([4, 8, 11] as $width) {
            yield (string) $width => ['width' => $width];
        }
    }

    public function depths(): Generator
    {
        foreach ([1, 10, 50] as $depth) {
            yield (string) $depth => ['depth' => $depth];
        }
    }

    public function limits(): Generator
    {
        yield 'none' => ['limit' => null];

        yield '1000' => ['limit' => 1000];
    }
}
