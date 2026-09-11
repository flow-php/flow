<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\IO;

use Flow\Parquet\Engine\ArrowParquetEngine;
use Flow\Parquet\Engine\PhpParquetEngine;
use Flow\Parquet\Tests\Context\TestParquetFile;
use PHPUnit\Framework\TestCase;

use function extension_loaded;

abstract class ParquetIntegrationTestCase extends TestCase
{
    /**
     * @return array<string, array{0: \Flow\Parquet\ParquetEngine}>
     */
    public static function engine_provider(): array
    {
        $engines = ['php' => [new PhpParquetEngine()]];

        if (extension_loaded('arrow')) {
            $engines['arrow'] = [new ArrowParquetEngine()];
        }

        return $engines;
    }

    protected function tearDown(): void
    {
        TestParquetFile::remove($this);
    }
}
