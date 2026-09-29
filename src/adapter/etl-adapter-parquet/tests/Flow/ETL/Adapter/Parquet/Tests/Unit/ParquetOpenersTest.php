<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Unit;

use Flow\ETL\Adapter\Parquet\EngineParquetOpener;
use Flow\ETL\Adapter\Parquet\NativeParquetOpener;
use Flow\ETL\Adapter\Parquet\ParquetOpeners;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Parquet\Engine\AdaptiveParquetEngine;
use Flow\Parquet\Engine\PhpParquetEngine;
use PHPUnit\Framework\Attributes\RequiresPhpExtension;

use function extension_loaded;

final class ParquetOpenersTest extends FlowTestCase
{
    public function test_a_given_engine_is_always_honoured(): void
    {
        $engine = new PhpParquetEngine();

        static::assertEquals(new EngineParquetOpener($engine), ParquetOpeners::select($engine));
    }

    #[RequiresPhpExtension('flow_php')]
    public function test_without_an_engine_flow_php_opens_the_files(): void
    {
        static::assertInstanceOf(NativeParquetOpener::class, ParquetOpeners::select(null));
    }

    public function test_without_an_engine_and_flow_php_the_adaptive_engine_opens_the_files(): void
    {
        if (extension_loaded('flow_php')) {
            static::markTestSkipped('flow_php is loaded');
        }

        static::assertEquals(new EngineParquetOpener(new AdaptiveParquetEngine()), ParquetOpeners::select(null));
    }
}
