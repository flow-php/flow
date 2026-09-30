<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet;

use Flow\Parquet\Binary\ByteOrder;
use Flow\Parquet\Engine\AdaptiveParquetEngine;
use Flow\Parquet\Engine\ArrowParquetEngine;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetEngine;

use function extension_loaded;

final class ParquetOpeners
{
    public static function select(?ParquetEngine $engine, ByteOrder $byteOrder, Options $options): ParquetOpener
    {
        if (
            extension_loaded('flow_php')
            && ($engine === null || $engine instanceof AdaptiveParquetEngine || $engine instanceof ArrowParquetEngine)
        ) {
            return new NativeParquetOpener($options);
        }

        return new EngineParquetOpener($engine ?? new AdaptiveParquetEngine($byteOrder, $options), $options);
    }
}
