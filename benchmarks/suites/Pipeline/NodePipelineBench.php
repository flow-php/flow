<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Pipeline;

use Flow\Benchmarks\BenchmarkRows;
use Generator;
use PhpBench\Attributes as Bench;

/**
 * XML and Text carry one column and do not infer, so these two numbers are comparable to each other
 * and to nothing else.
 */
#[Bench\BeforeMethods('warm')]
final class NodePipelineBench
{
    public function warm(array $params): void
    {
        (new NodeFixture(NodeSource::from((string) $params['source']), (int) $params['rows']))->warm();
    }

    #[Bench\ParamProviders(['rows', 'source_xml'])]
    #[Bench\Groups(['pipeline-node', 'pipeline-xml'])]
    public function bench_pipeline_xml(array $params): void
    {
        (new NodePipelineScenario(NodeSource::xml, (int) $params['rows']))->run();
    }

    #[Bench\ParamProviders(['rows', 'source_text'])]
    #[Bench\Groups(['pipeline-node', 'pipeline-text'])]
    public function bench_pipeline_text(array $params): void
    {
        (new NodePipelineScenario(NodeSource::text, (int) $params['rows']))->run();
    }

    public function rows(): Generator
    {
        $rows = BenchmarkRows::count();

        yield number_format($rows) => ['rows' => $rows];
    }

    public function source_xml(): Generator
    {
        yield 'xml' => ['source' => NodeSource::xml->value];
    }

    public function source_text(): Generator
    {
        yield 'text' => ['source' => NodeSource::text->value];
    }
}
