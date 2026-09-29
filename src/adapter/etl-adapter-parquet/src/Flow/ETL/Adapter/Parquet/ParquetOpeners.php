<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet;

use Flow\Parquet\Engine\AdaptiveParquetEngine;
use Flow\Parquet\ParquetEngine;

use function extension_loaded;

final class ParquetOpeners
{
    public static function select(?ParquetEngine $engine): ParquetOpener
    {
        return match (true) {
            $engine !== null => new EngineParquetOpener($engine),
            extension_loaded('flow_php') => new NativeParquetOpener(),
            default => new EngineParquetOpener(new AdaptiveParquetEngine()),
        };
    }
}
