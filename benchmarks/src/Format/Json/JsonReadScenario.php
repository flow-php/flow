<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Format\Json;

use Flow\Benchmarks\Datasets\Datasets;

use function Flow\ETL\Adapter\JSON\from_json;
use function Flow\ETL\DSL\data_frame;

final readonly class JsonReadScenario
{
    public function __construct(
        private int $rows,
    ) {}

    public function run(): void
    {
        data_frame()->read(from_json(Datasets::orders($this->rows)->json()))->run();
    }
}
