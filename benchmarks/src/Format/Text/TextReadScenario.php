<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Format\Text;

use Flow\Benchmarks\Datasets\Datasets;

use function Flow\ETL\Adapter\Text\from_text;
use function Flow\ETL\DSL\data_frame;

final readonly class TextReadScenario
{
    public function __construct(
        private int $rows,
    ) {}

    public function run(): void
    {
        data_frame()->read(from_text(Datasets::text($this->rows)->path()))->run();
    }
}
