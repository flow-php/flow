<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Format\Floe;

use Flow\Benchmarks\Datasets\Datasets;
use Flow\Benchmarks\Datasets\Paths;
use Flow\ETL\Row\PhpRowHydrator;
use Flow\Floe\FloeEngine;

use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\data_frame;
use function Flow\Floe\DSL\from_floe;
use function Flow\Floe\DSL\to_floe;
use function uniqid;

final readonly class FloeWriteScenario
{
    public function __construct(
        private int $rows,
        private FloeEngine $engine,
    ) {}

    public function run(): void
    {
        $config = config_builder();

        if ($this->engine === FloeEngine::php) {
            $config->hydrator(new PhpRowHydrator());
        }

        data_frame($config)
            ->read(from_floe(Datasets::orders($this->rows)->floe(), engine: $this->engine))
            ->write(to_floe(Paths::var() . '/format_write_floe_' . uniqid('', true) . '.floe', engine: $this->engine))
            ->run();
    }
}
