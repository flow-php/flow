<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Format\Text;

use Flow\Benchmarks\BenchmarkRows;
use Flow\Benchmarks\Datasets\Datasets;
use Generator;
use PhpBench\Attributes as Bench;

#[Bench\BeforeMethods('warm')]
final class TextReadBench
{
    public function warm(array $params): void
    {
        Datasets::text((int) $params['rows'])->path();
    }

    #[Bench\ParamProviders('rows')]
    #[Bench\Groups(['format', 'format-text'])]
    public function bench_text_read(array $params): void
    {
        (new TextReadScenario((int) $params['rows']))->run();
    }

    public function rows(): Generator
    {
        $rows = BenchmarkRows::count();

        yield number_format($rows) => ['rows' => $rows];
    }
}
