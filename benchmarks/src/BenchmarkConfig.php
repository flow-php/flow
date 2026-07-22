<?php

declare(strict_types=1);

namespace Flow\Benchmarks;

use Flow\Benchmarks\Datasets\Paths;
use Flow\ETL\Config\ConfigBuilder;

use function Flow\ETL\DSL\config_builder;

final class BenchmarkConfig
{
    public static function builder(): ConfigBuilder
    {
        return config_builder()->cacheDir(Paths::var());
    }
}
