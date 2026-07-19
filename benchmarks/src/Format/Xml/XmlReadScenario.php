<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Format\Xml;

use Flow\Benchmarks\Datasets\Datasets;

use function Flow\ETL\Adapter\XML\from_xml;
use function Flow\ETL\DSL\data_frame;

final readonly class XmlReadScenario
{
    public function __construct(
        private int $rows,
    ) {}

    public function run(): void
    {
        data_frame()->read(from_xml(Datasets::orders($this->rows)->xml()))->run();
    }
}
