<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Engine;

use Flow\Parquet\Engine\AdaptiveParquetEngine;
use Flow\Parquet\Engine\ArrowParquetEngine;
use Flow\Parquet\Engine\PhpParquetEngine;
use PHPUnit\Framework\TestCase;
use ReflectionClass;

use function extension_loaded;

final class AdaptiveParquetEngineTest extends TestCase
{
    public function test_creates_arrow_engine_when_flow_php_or_arrow_is_loaded(): void
    {
        if (!extension_loaded('flow_php') && !extension_loaded('arrow')) {
            static::markTestSkipped('This test requires flow_php or arrow to be loaded');
        }

        $engine = new AdaptiveParquetEngine();

        static::assertInstanceOf(
            ArrowParquetEngine::class,
            (new ReflectionClass($engine))
                ->getProperty('delegate')
                ->getValue($engine),
        );
    }

    public function test_creates_php_engine_without_flow_php_and_arrow(): void
    {
        if (extension_loaded('flow_php') || extension_loaded('arrow')) {
            static::markTestSkipped('This test requires neither flow_php nor arrow to be loaded');
        }

        $engine = new AdaptiveParquetEngine();

        static::assertInstanceOf(
            PhpParquetEngine::class,
            (new ReflectionClass($engine))
                ->getProperty('delegate')
                ->getValue($engine),
        );
    }
}
