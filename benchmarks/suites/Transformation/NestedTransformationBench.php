<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Transformation;

use Flow\Benchmarks\BenchmarkRows;
use Flow\Benchmarks\Datasets\Datasets;
use Generator;
use PhpBench\Attributes as Bench;

use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\select;
use function Flow\ETL\DSL\to_branch;
use function Flow\ETL\DSL\to_transformation;

#[Bench\BeforeMethods('warm')]
final class NestedTransformationBench
{
    public function warm(array $params): void
    {
        Datasets::orders((int) $params['rows'])->floe();
    }

    #[Bench\ParamProviders('rows')]
    #[Bench\Groups(['transformation'])]
    public function bench_blocking_transformation(array $params): void
    {
        $loader = to_transformation(new SortByCreatedAt(), new NoopLoader());

        (new NestedTransformationScenario((int) $params['rows'], $loader))->run();
    }

    #[Bench\ParamProviders('rows')]
    #[Bench\Groups(['transformation'])]
    public function bench_branch_with_transformation(array $params): void
    {
        $loader = to_branch(ref('order_id')->isNotNull(), new NoopLoader())->withTransformation(new SortByCreatedAt());

        (new NestedTransformationScenario((int) $params['rows'], $loader))->run();
    }

    #[Bench\ParamProviders('rows')]
    #[Bench\Groups(['transformation'])]
    public function bench_streaming_transformation(array $params): void
    {
        $loader = to_transformation(select('order_id'), new NoopLoader());

        (new NestedTransformationScenario((int) $params['rows'], $loader))->run();
    }

    public function rows(): Generator
    {
        $rows = BenchmarkRows::count();

        yield number_format($rows) => ['rows' => $rows];
    }
}
