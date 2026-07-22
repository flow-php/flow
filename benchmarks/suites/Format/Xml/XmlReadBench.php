<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Format\Xml;

use Generator;
use PhpBench\Attributes as Bench;

final class XmlReadBench
{
    #[Bench\ParamProviders('rows')]
    #[Bench\Groups(['format', 'format-xml'])]
    public function bench_xml_read(array $params): void
    {
        (new XmlReadScenario((int) $params['rows']))->run();
    }

    public function rows(): Generator
    {
        $rows = (int) (getenv('FLOW_BENCH_ROWS') ?: 100_000);

        yield number_format($rows) => ['rows' => $rows];
    }
}
