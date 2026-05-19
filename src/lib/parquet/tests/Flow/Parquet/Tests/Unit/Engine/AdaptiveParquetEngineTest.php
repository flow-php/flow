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
    public function test_creates_arrow_engine_when_arrow_extension_loaded(): void
    {
        if (!extension_loaded('arrow')) {
            static::markTestSkipped('This test requires the arrow extension to be loaded');
        }

        $engine = new AdaptiveParquetEngine();

        static::assertInstanceOf(
            ArrowParquetEngine::class,
            (new ReflectionClass($engine))
                ->getProperty('delegate')
                ->getValue($engine),
        );
    }

    public function test_creates_php_engine_when_arrow_extension_not_loaded(): void
    {
        if (extension_loaded('arrow')) {
            static::markTestSkipped('This test requires the arrow extension to NOT be loaded');
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
