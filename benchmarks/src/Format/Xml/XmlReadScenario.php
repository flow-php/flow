<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Format\Xml;

use Flow\Benchmarks\Datasets\Datasets;

use function Flow\ETL\Adapter\XML\from_xml;
use function Flow\ETL\DSL\data_frame;

/**
 * The node path is load-bearing: without it from_xml() yields ONE row holding the entire
 * document, so the subject would not scale with FLOW_BENCH_ROWS at all.
 */
final readonly class XmlReadScenario
{
    public function __construct(
        private int $rows,
    ) {}

    public function run(): void
    {
        data_frame()->read(from_xml(Datasets::orders($this->rows)->xml(), 'rows/row'))->run();
    }
}
