<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Pipeline;

use Flow\Benchmarks\BenchmarkConfig;
use Flow\Benchmarks\Datasets\Paths;

use function Flow\ETL\Adapter\Text\from_text;
use function Flow\ETL\Adapter\Text\to_text;
use function Flow\ETL\Adapter\XML\from_xml;
use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\xml_schema;
use function Flow\Types\DSL\type_string;
use function uniqid;

/**
 * XML and Text do not infer and cannot carry the orders schema, so they form their own family: the
 * same four verbs in the same order as the canonical shape, over one column.
 *
 * These two numbers are comparable to each other and to nothing else. The sink is to_text because a
 * one-column string frame in Floe would measure Floe's footer rather than the pipeline.
 */
final readonly class NodePipelineScenario
{
    /**
     * Without it from_xml() yields ONE row holding the whole document at every fixture size, so the
     * subject would not scale with FLOW_BENCH_ROWS and would not be comparable to the Text subject.
     */
    private const XML_NODE_PATH = 'rows/row';

    public function __construct(
        private NodeSource $source,
        private int $rows,
    ) {}

    public function run(): void
    {
        $frame = match ($this->source) {
            NodeSource::xml => data_frame(BenchmarkConfig::builder())
                ->read(from_xml($this->source->path($this->rows), self::XML_NODE_PATH)->withSchema(schema(xml_schema(
                    'node',
                ))))
                ->withEntry('body', ref('node')->cast(type_string())),
            NodeSource::text => data_frame(BenchmarkConfig::builder())
                ->read(from_text($this->source->path($this->rows))->withSchema(schema(str_schema('text'))))
                ->withEntry('body', ref('text')->concat(lit(' |'))),
        };

        $frame
            ->filter(ref('body')->isNotNull())
            ->select('body')
            ->write(to_text(Paths::var() . '/pipeline_' . $this->source->value . '_' . uniqid('', true) . '.txt'))
            ->run();
    }
}
