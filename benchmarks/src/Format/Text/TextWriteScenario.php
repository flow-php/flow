<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Format\Text;

use Flow\Benchmarks\Datasets\Datasets;
use Flow\Benchmarks\Datasets\Paths;

use function Flow\ETL\Adapter\Text\to_text;
use function Flow\ETL\DSL\data_frame;
use function Flow\Floe\DSL\from_floe;
use function uniqid;

final readonly class TextWriteScenario
{
    public function __construct(
        private int $rows,
    ) {}

    public function run(): void
    {
        data_frame()
            ->read(from_floe(Datasets::orders($this->rows)->floe()))
            ->select('customer')
            ->write(to_text(Paths::var() . '/format_write_text_' . uniqid('', true) . '.txt'))
            ->run();
    }
}
