<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Format\Text;

use Generator;
use PhpBench\Attributes as Bench;

final class TextReadBench
{
    #[Bench\ParamProviders('rows')]
    #[Bench\Groups(['format', 'format-text'])]
    public function bench_text_read(array $params): void
    {
        (new TextReadScenario((int) $params['rows']))->run();
    }

    public function rows(): Generator
    {
        $rows = (int) (getenv('FLOW_BENCH_ROWS') ?: 100_000);

        yield number_format($rows) => ['rows' => $rows];
    }
}
