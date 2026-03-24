<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\IO;

use Flow\Parquet\Engine\{ArrowParquetEngine, PhpParquetEngine};
use PHPUnit\Framework\TestCase;

abstract class ParquetIntegrationTestCase extends TestCase
{
    public static function engine_provider() : array
    {
        $engines = ['php' => [new PhpParquetEngine()]];

        if (\extension_loaded('arrow')) {
            $engines['arrow'] = [new ArrowParquetEngine()];
        }

        return $engines;
    }
}
