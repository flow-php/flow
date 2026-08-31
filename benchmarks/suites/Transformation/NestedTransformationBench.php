<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Transformation;

use Generator;
use PhpBench\Attributes as Bench;

use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\select;
use function Flow\ETL\DSL\to_branch;
use function Flow\ETL\DSL\to_transformation;

final class NestedTransformationBench
{
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
        $rows = (int) (getenv('FLOW_BENCH_ROWS') ?: 100_000);

        yield number_format($rows) => ['rows' => $rows];
    }
}
