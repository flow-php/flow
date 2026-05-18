<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit;

use Flow\Parquet\Engine\PhpParquetEngine;
use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\Writer;
use PHPUnit\Framework\TestCase;
use ReflectionClass;

use function extension_loaded;

final class WriterTest extends TestCase
{
    public function test_arrow_factory_throws_when_extension_not_loaded(): void
    {
        if (extension_loaded('arrow')) {
            static::markTestSkipped('This test requires the arrow extension to NOT be loaded');
        }

        $this->expectException(RuntimeException::class);

        Writer::arrow();
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
