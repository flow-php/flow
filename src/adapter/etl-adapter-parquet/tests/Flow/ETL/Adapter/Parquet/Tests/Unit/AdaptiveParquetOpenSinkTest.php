<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Unit;

use Closure;
use Flow\ETL\Adapter\Parquet\AdaptiveParquetOpenSink;
use Flow\ETL\Adapter\Parquet\SchemaConverter;
use Flow\ETL\Adapter\Parquet\Tests\Context\ParquetFilesContext;
use Flow\ETL\Adapter\Parquet\Tests\Double\RecordingParquetEngine;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\FlowPhpExtension;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Parquet\Binary\ByteOrder;
use Flow\Parquet\Engine\AdaptiveParquetEngine;
use Flow\Parquet\Engine\PhpParquetEngine;
use Flow\Parquet\Engine\RustParquetEngine;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetEngine;
use Flow\Parquet\ParquetFile\Compressions;
use PHPUnit\Framework\Attributes\DataProvider;

use function extension_loaded;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;

final class AdaptiveParquetOpenSinkTest extends FlowTestCase
{
    /**
     * @return iterable<string, array{?ParquetEngine}>
     */
    public static function engines(): iterable
    {
        yield 'no engine' => [null];
        yield 'adaptive engine' => [new AdaptiveParquetEngine()];
    }

    #[DataProvider('engines')]
    public function test_a_default_engine_writes_a_file_php_reads(?ParquetEngine $engine): void
    {
        $filesystem = memory_filesystem();
        $schema = schema(int_schema('id'));
        $sink = new AdaptiveParquetOpenSink(
            $filesystem->writeTo(path('memory://out.parquet')),
            (new SchemaConverter())->toParquet($schema),
            Compressions::GZIP,
            Options::default(),
            $engine,
        );

        $sink->write(array_to_rows([['id' => 1], ['id' => 2]], $schema));
        $sink->close();

        static::assertSame(
            [['id' => 1], ['id' => 2]],
            ParquetFilesContext::phpEngineValues($filesystem, 'memory://out.parquet'),
        );
    }

    /**
     * The refusal of a string written into an int column names the lane that wrote: flow_php's native columns into
     * arrow-ext, PHP values into arrow-ext, or PHP values into the PHP engine.
     *
     * @return iterable<string, array{Closure(): ?ParquetEngine, string}>
     */
    public static function lanes(): iterable
    {
        $arrow = extension_loaded('arrow');
        $rust = match (true) {
            $arrow && extension_loaded('flow_php') => 'is not supported by the arrow Parquet writer',
            $arrow => 'row 0: expected int, got string',
            default => 'require integer as value',
        };

        yield 'no engine' => [static fn(): ?ParquetEngine => null, $rust];
        yield 'little-endian adaptive engine' => [static fn(): ParquetEngine => new AdaptiveParquetEngine(), $rust];
        yield 'big-endian adaptive engine' => [
            static fn(): ParquetEngine => new AdaptiveParquetEngine(ByteOrder::BIG_ENDIAN),
            'require integer as value',
        ];
        yield 'php engine' => [static fn(): ParquetEngine => new PhpParquetEngine(), 'require integer as value'];

        if ($arrow) {
            yield 'rust engine' => [static fn(): ParquetEngine => new RustParquetEngine(), $rust];
        }
    }

    /**
     * @param Closure(): ?ParquetEngine $engine
     */
    #[DataProvider('lanes')]
    public function test_the_engine_decides_which_lane_writes(Closure $engine, string $refusal): void
    {
        $sink = new AdaptiveParquetOpenSink(
            memory_filesystem()->writeTo(path('memory://out.parquet')),
            (new SchemaConverter())->toParquet(schema(int_schema('id'))),
            Compressions::SNAPPY,
            Options::default(),
            $engine(),
        );

        $this->expectExceptionMessage($refusal);

        $sink->write(array_to_rows([['id' => 'one']], schema(str_schema('id'))));
    }

    public function test_any_other_engine_writes_the_file(): void
    {
        $filesystem = memory_filesystem();
        $schema = schema(int_schema('id'));
        $engine = new RecordingParquetEngine();
        $sink = new AdaptiveParquetOpenSink(
            $filesystem->writeTo(path('memory://out.parquet')),
            (new SchemaConverter())->toParquet($schema),
            Compressions::SNAPPY,
            Options::default(),
            $engine,
        );

        $sink->write(array_to_rows([['id' => 1], ['id' => 2]], $schema));
        $sink->close();

        static::assertSame(1, $engine->openedForWrite);
        static::assertSame(
            [['id' => 1], ['id' => 2]],
            ParquetFilesContext::phpEngineValues($filesystem, 'memory://out.parquet'),
        );
    }

    public function test_a_flow_php_of_another_abi_is_refused(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('does not match flow-php/etl');

        new AdaptiveParquetOpenSink(
            memory_filesystem()->writeTo(path('memory://out.parquet')),
            (new SchemaConverter())->toParquet(schema(int_schema('id'))),
            Compressions::SNAPPY,
            Options::default(),
            new PhpParquetEngine(),
            new FlowPhpExtension(true, FlowPhpExtension::ABI + 1, '0.46.0'),
        );
    }
}
