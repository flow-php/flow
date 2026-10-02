<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Unit;

use Flow\ETL\Adapter\Parquet\EngineParquetOpener;
use Flow\ETL\Adapter\Parquet\NativeParquetOpener;
use Flow\ETL\Adapter\Parquet\ParquetOpeners;
use Flow\ETL\Adapter\Parquet\Tests\Context\ParquetSourceFileContext;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Stream\NativeLocalSourceStream;
use Flow\Parquet\Binary\ByteOrder;
use Flow\Parquet\Engine\AdaptiveParquetEngine;
use Flow\Parquet\Engine\ArrowParquetEngine;
use Flow\Parquet\Engine\ArrowParquetFileReader;
use Flow\Parquet\Engine\PhpParquetEngine;
use Flow\Parquet\Engine\PhpParquetFileReader;
use Flow\Parquet\Options;
use PHPUnit\Framework\Attributes\RequiresPhpExtension;

use function extension_loaded;

final class ParquetOpenersTest extends FlowTestCase
{
    public function test_a_php_engine_is_always_honoured(): void
    {
        $engine = new PhpParquetEngine();

        static::assertEquals(
            new EngineParquetOpener($engine, Options::default()),
            ParquetOpeners::select($engine, ByteOrder::LITTLE_ENDIAN, Options::default()),
        );
    }

    #[RequiresPhpExtension('flow_php')]
    #[RequiresPhpExtension('arrow')]
    public function test_with_both_extensions_an_adaptive_engine_reads_native_columns(): void
    {
        static::assertEquals(
            new NativeParquetOpener(Options::default()),
            ParquetOpeners::select(new AdaptiveParquetEngine(), ByteOrder::LITTLE_ENDIAN, Options::default()),
        );
    }

    #[RequiresPhpExtension('flow_php')]
    #[RequiresPhpExtension('arrow')]
    public function test_with_both_extensions_an_arrow_engine_reads_native_columns(): void
    {
        static::assertEquals(
            new NativeParquetOpener(Options::default()),
            ParquetOpeners::select(new ArrowParquetEngine(), ByteOrder::LITTLE_ENDIAN, Options::default()),
        );
    }

    #[RequiresPhpExtension('flow_php')]
    #[RequiresPhpExtension('arrow')]
    public function test_with_both_extensions_no_engine_reads_native_columns(): void
    {
        static::assertEquals(
            new NativeParquetOpener(Options::default()),
            ParquetOpeners::select(null, ByteOrder::LITTLE_ENDIAN, Options::default()),
        );
    }

    public function test_without_both_extensions_an_adaptive_engine_is_honoured(): void
    {
        if (extension_loaded('flow_php') && extension_loaded('arrow')) {
            static::markTestSkipped('flow_php and arrow are loaded');
        }

        $engine = new AdaptiveParquetEngine();

        static::assertEquals(
            new EngineParquetOpener($engine, Options::default()),
            ParquetOpeners::select($engine, ByteOrder::LITTLE_ENDIAN, Options::default()),
        );
    }

    #[RequiresPhpExtension('arrow')]
    public function test_with_arrow_alone_an_arrow_engine_is_honoured(): void
    {
        if (extension_loaded('flow_php')) {
            static::markTestSkipped('flow_php is loaded');
        }

        $engine = new ArrowParquetEngine();

        static::assertEquals(
            new EngineParquetOpener($engine, Options::default()),
            ParquetOpeners::select($engine, ByteOrder::LITTLE_ENDIAN, Options::default()),
        );
    }

    public function test_without_both_extensions_no_engine_opens_through_an_adaptive_engine(): void
    {
        if (extension_loaded('flow_php') && extension_loaded('arrow')) {
            static::markTestSkipped('flow_php and arrow are loaded');
        }

        static::assertEquals(
            new EngineParquetOpener(
                new AdaptiveParquetEngine(ByteOrder::BIG_ENDIAN, Options::default()),
                Options::default(),
            ),
            ParquetOpeners::select(null, ByteOrder::BIG_ENDIAN, Options::default()),
        );
    }

    public function test_an_engine_opener_opens_files_with_its_engine_reader(): void
    {
        $file = (new EngineParquetOpener(new PhpParquetEngine(), Options::default()))->file(
            NativeLocalSourceStream::open(ParquetSourceFileContext::fixture()),
        );

        static::assertInstanceOf(PhpParquetFileReader::class, $file->reader());
    }

    #[RequiresPhpExtension('flow_php')]
    #[RequiresPhpExtension('arrow')]
    public function test_a_native_opener_opens_files_with_an_arrow_reader(): void
    {
        $file = (new NativeParquetOpener(Options::default()))->file(
            NativeLocalSourceStream::open(ParquetSourceFileContext::fixture()),
        );

        static::assertInstanceOf(ArrowParquetFileReader::class, $file->reader());
    }
}
