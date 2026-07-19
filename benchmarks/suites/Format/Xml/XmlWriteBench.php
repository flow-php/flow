<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Format\Xml;

use Generator;
use PhpBench\Attributes as Bench;

final class XmlWriteBench
{
    #[Bench\ParamProviders('rows')]
    #[Bench\Groups(['format', 'format-xml'])]
    public function bench_xml_write(array $params): void
    {
        (new XmlWriteScenario((int) $params['rows']))->run();
    }

    public function rows(): Generator
    {
        $rows = (int) (getenv('FLOW_BENCH_ROWS') ?: 100_000);

        yield number_format($rows) => ['rows' => $rows];
    }
}
