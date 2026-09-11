<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Schema;

use Flow\Benchmarks\Datasets\Datasets;
use Flow\ETL\Schema;

use function Flow\ETL\DSL\data_frame;
use function Flow\Floe\DSL\from_floe;

/**
 * A self-describing format answers schema() from its footer without reading a row — the counterpart
 * to SchemaInferenceScenario's sampling pass.
 */
final readonly class FooterSchemaScenario
{
    public function __construct(
        private int $rows,
    ) {}

    public function run(): Schema
    {
        return data_frame()->read(from_floe(Datasets::orders($this->rows)->floe()))->schema();
    }
}
