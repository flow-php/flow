<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Unit\IO;

use Flow\ETL\Adapter\Parquet\IO\ConfiguredParquetEngine;
use Flow\ETL\Adapter\Parquet\Tests\Double\RefusingParquetEngine;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Parquet\Engine\PhpParquetEngine;
use Flow\Parquet\Option;
use Flow\Parquet\Options;

use function array_filter;
use function extension_loaded;
use function sort;

final class ConfiguredParquetEngineTest extends FlowTestCase
{
    public function test_an_engine_round_trips_without_secrecy(): void
    {
        $engine = new PhpParquetEngine(options: Options::default()->set(Option::PAGE_SIZE_BYTES, 7));
        $description = (new ConfiguredParquetEngine($engine))->describe();

        static::assertNotNull($description);
        static::assertFalse($description->sensitive());
        static::assertSame(PhpParquetEngine::class, $description->data['engine']);
        static::assertEquals(
            $engine,
            (new Rebuild())->of($description->normalize(), ConfiguredParquetEngine::class)->engine(),
        );
    }

    public function test_an_engine_without_a_normal_form_does_not_describe(): void
    {
        static::assertNull((new ConfiguredParquetEngine(new RefusingParquetEngine()))->describe());
    }

    public function test_a_class_that_is_not_an_engine_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Flow\ETL\Foundation\Describe\Rebuild is not a concrete ParquetEngine');

        ConfiguredParquetEngine::fromDescription(['engine' => Rebuild::class, 'configuration' => []], new Rebuild());
    }

    public function test_an_engine_declares_the_parquet_extensions_and_the_codecs_loaded_here(): void
    {
        $expected = [
            'bcmath',
            'json',
            'zlib',
            ...array_filter(['brotli', 'lz4', 'snappy', 'zstd'], extension_loaded(...)),
        ];
        sort($expected);

        static::assertSame($expected, (new ConfiguredParquetEngine(new PhpParquetEngine()))->describe()?->extensions());
    }
}
