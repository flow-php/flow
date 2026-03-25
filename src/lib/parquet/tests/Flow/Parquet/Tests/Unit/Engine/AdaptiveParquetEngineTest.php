<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Engine;

use Flow\Parquet\Engine\{AdaptiveParquetEngine, ArrowParquetEngine, PhpParquetEngine};
use PHPUnit\Framework\TestCase;

final class AdaptiveParquetEngineTest extends TestCase
{
    public function test_creates_arrow_engine_when_arrow_extension_loaded() : void
    {
        if (!\extension_loaded('arrow')) {
            self::markTestSkipped('This test requires the arrow extension to be loaded');
        }

        $engine = new AdaptiveParquetEngine();

        $reflection = new \ReflectionClass($engine);
        $delegate = $reflection->getProperty('delegate')->getValue($engine);

        self::assertInstanceOf(ArrowParquetEngine::class, $delegate);
    }

    public function test_creates_php_engine_when_arrow_extension_not_loaded() : void
    {
        if (\extension_loaded('arrow')) {
            self::markTestSkipped('This test requires the arrow extension to NOT be loaded');
        }

        $engine = new AdaptiveParquetEngine();

        $reflection = new \ReflectionClass($engine);
        $delegate = $reflection->getProperty('delegate')->getValue($engine);

        self::assertInstanceOf(PhpParquetEngine::class, $delegate);
    }
}
