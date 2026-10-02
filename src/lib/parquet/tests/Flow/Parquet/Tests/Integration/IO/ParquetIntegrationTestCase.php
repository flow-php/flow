<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\IO;

use Flow\Parquet\Engine\PhpParquetEngine;
use Flow\Parquet\Engine\RustParquetEngine;
use Flow\Parquet\ParquetEngine;
use Flow\Parquet\Tests\Context\TestParquetFile;
use PHPUnit\Framework\TestCase;

use function extension_loaded;

abstract class ParquetIntegrationTestCase extends TestCase
{
    /**
     * @return array<string, array{class-string<ParquetEngine>}>
     */
    public static function engine_provider(): array
    {
        $engines = ['php' => [PhpParquetEngine::class]];

        if (extension_loaded('arrow')) {
            $engines['arrow'] = [RustParquetEngine::class];
        }

        return $engines;
    }

    /**
     * @return array<string, array{class-string<ParquetEngine>, class-string<ParquetEngine>}>
     */
    public static function engine_pair_provider(): array
    {
        $pairs = ['php/php' => [PhpParquetEngine::class, PhpParquetEngine::class]];

        if (extension_loaded('arrow')) {
            $pairs['php/arrow'] = [PhpParquetEngine::class, RustParquetEngine::class];
            $pairs['arrow/php'] = [RustParquetEngine::class, PhpParquetEngine::class];
            $pairs['arrow/arrow'] = [RustParquetEngine::class, RustParquetEngine::class];
        }

        return $pairs;
    }

    protected function tearDown(): void
    {
        TestParquetFile::remove($this);
    }
}
