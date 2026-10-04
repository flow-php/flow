<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit;

use Flow\Parquet\Engine\AdaptiveParquetEngine;
use Flow\Parquet\Engine\PhpParquetEngine;
use Flow\Parquet\Writer;
use PHPUnit\Framework\TestCase;
use ReflectionClass;

final class WriterTest extends TestCase
{
    public function test_the_default_engine_writes(): void
    {
        $writer = new Writer();

        static::assertInstanceOf(
            AdaptiveParquetEngine::class,
            (new ReflectionClass($writer))
                ->getProperty('engine')
                ->getValue($writer),
        );
    }

    public function test_php_factory_creates_writer_with_php_engine(): void
    {
        $writer = Writer::php();

        static::assertInstanceOf(
            PhpParquetEngine::class,
            (new ReflectionClass($writer))
                ->getProperty('engine')
                ->getValue($writer),
        );
    }
}
