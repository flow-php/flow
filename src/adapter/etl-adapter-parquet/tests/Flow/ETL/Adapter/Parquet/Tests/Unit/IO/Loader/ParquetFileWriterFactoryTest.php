<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Unit\IO\Loader;

use Closure;
use Flow\ETL\Adapter\Parquet\IO\Loader\ParquetFileWriter;
use Flow\ETL\Adapter\Parquet\IO\Loader\ParquetFileWriterFactory;
use Flow\ETL\Adapter\Parquet\IO\Loader\RustParquetFileWriter;
use Flow\ETL\Adapter\Parquet\Tests\Context\ParquetFilesContext;
use Flow\ETL\Adapter\Parquet\Tests\Double\RecordingParquetEngine;
use Flow\ETL\Adapter\Parquet\Values\Schema\SchemaConverter;
use Flow\ETL\Foundation\Exception\RuntimeException;
use Flow\ETL\Foundation\FlowPhpExtension;
use Flow\ETL\IO\Loader\File\FileWriter;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Column\PhpBackend;
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

final class ParquetFileWriterFactoryTest extends FlowTestCase
{
    /**
     * @return iterable<string, array{?ParquetEngine}>
     */
    public static function engines(): iterable
    {
        yield 'no engine' => [null];
        yield 'adaptive engine' => [new AdaptiveParquetEngine()];
    }

    /**
     * flow_php's native lane writes only into arrow-ext's writer; every other writer takes PHP columns.
     *
     * @return iterable<string, array{Closure(): ?ParquetEngine, class-string<FileWriter>}>
     */
    public static function lanes(): iterable
    {
        $arrow = extension_loaded('arrow');
        $rust = $arrow && FlowPhpExtension::detect()->available()
            ? RustParquetFileWriter::class
            : ParquetFileWriter::class;

        yield 'no engine' => [static fn(): ?ParquetEngine => null, $rust];
        yield 'little-endian adaptive engine' => [static fn(): ParquetEngine => new AdaptiveParquetEngine(), $rust];
        yield 'big-endian adaptive engine' => [
            static fn(): ParquetEngine => new AdaptiveParquetEngine(ByteOrder::BIG_ENDIAN),
            ParquetFileWriter::class,
        ];
        yield 'php engine' => [static fn(): ParquetEngine => new PhpParquetEngine(), ParquetFileWriter::class];

        if ($arrow) {
            yield 'rust engine' => [static fn(): ParquetEngine => new RustParquetEngine(), $rust];
        }
    }

    public function test_the_first_writer_fixes_the_schema_every_stream_is_written_under(): void
    {
        $filesystem = memory_filesystem();
        $writers = new ParquetFileWriterFactory(
            null,
            new SchemaConverter(),
            Compressions::UNCOMPRESSED,
            Options::default(),
            new PhpParquetEngine(),
        );
        $first = $writers->writer(
            $filesystem->writeTo(path('memory://a.parquet')),
            schema(int_schema('id'), str_schema('name')),
            new PhpBackend(),
        );
        $second = $writers->writer(
            $filesystem->writeTo(path('memory://b.parquet')),
            schema(int_schema('id')),
            new PhpBackend(),
        );

        $first->write(array_to_rows([['id' => 1, 'name' => 'a']], schema(int_schema('id'), str_schema('name'))));
        $second->write(array_to_rows([['id' => 2]], schema(int_schema('id'))));
        $first->close();
        $second->close();

        static::assertSame(['id', 'name'], ParquetFilesContext::columnNames($filesystem, 'memory://b.parquet'));
        static::assertSame(
            [['id' => 2, 'name' => null]],
            ParquetFilesContext::values($filesystem, 'memory://b.parquet'),
        );
    }

    public function test_a_declared_schema_is_the_written_one(): void
    {
        $filesystem = memory_filesystem();
        $writer = (new ParquetFileWriterFactory(
            schema(int_schema('id', nullable: true), str_schema('note', nullable: true)),
            new SchemaConverter(),
            Compressions::UNCOMPRESSED,
            Options::default(),
            new PhpParquetEngine(),
        ))->writer($filesystem->writeTo(path('memory://a.parquet')), schema(int_schema('id')), new PhpBackend());

        $writer->write(array_to_rows([['id' => 1]], schema(int_schema('id'))));
        $writer->close();

        static::assertSame(['id', 'note'], ParquetFilesContext::columnNames($filesystem, 'memory://a.parquet'));
    }

    public function test_every_writer_opens_its_stream_at_once(): void
    {
        $filesystem = memory_filesystem();
        $engine = new RecordingParquetEngine();
        $writers = new ParquetFileWriterFactory(
            schema(int_schema('id')),
            new SchemaConverter(),
            Compressions::UNCOMPRESSED,
            Options::default(),
            $engine,
        );

        $writers->writer($filesystem->writeTo(path('memory://a.parquet')), schema(int_schema('id')), new PhpBackend());
        $writers->writer($filesystem->writeTo(path('memory://b.parquet')), schema(int_schema('id')), new PhpBackend());

        static::assertSame(2, $engine->openedForWrite);
    }

    #[DataProvider('engines')]
    public function test_a_default_engine_writes_a_file_php_reads(?ParquetEngine $engine): void
    {
        $filesystem = memory_filesystem();
        $schema = schema(int_schema('id'));
        $writer = (new ParquetFileWriterFactory(
            $schema,
            new SchemaConverter(),
            Compressions::GZIP,
            Options::default(),
            $engine,
        ))->writer($filesystem->writeTo(path('memory://out.parquet')), $schema, new PhpBackend());

        $writer->write(array_to_rows([['id' => 1], ['id' => 2]], $schema));
        $writer->close();

        static::assertSame(
            [['id' => 1], ['id' => 2]],
            ParquetFilesContext::phpEngineValues($filesystem, 'memory://out.parquet'),
        );
    }

    /**
     * @param Closure(): ?ParquetEngine $engine
     * @param class-string<FileWriter> $lane
     */
    #[DataProvider('lanes')]
    public function test_the_engine_decides_which_lane_writes(Closure $engine, string $lane): void
    {
        static::assertInstanceOf($lane, (new ParquetFileWriterFactory(
            schema(int_schema('id')),
            new SchemaConverter(),
            Compressions::SNAPPY,
            Options::default(),
            $engine(),
        ))->writer(
            memory_filesystem()->writeTo(path('memory://out.parquet')),
            schema(int_schema('id')),
            new PhpBackend(),
        ));
    }

    public function test_any_other_engine_writes_the_file(): void
    {
        $filesystem = memory_filesystem();
        $schema = schema(int_schema('id'));
        $engine = new RecordingParquetEngine();
        $writer = (new ParquetFileWriterFactory(
            $schema,
            new SchemaConverter(),
            Compressions::SNAPPY,
            Options::default(),
            $engine,
        ))->writer($filesystem->writeTo(path('memory://out.parquet')), $schema, new PhpBackend());

        $writer->write(array_to_rows([['id' => 1], ['id' => 2]], $schema));
        $writer->close();

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

        (new ParquetFileWriterFactory(
            schema(int_schema('id')),
            new SchemaConverter(),
            Compressions::SNAPPY,
            Options::default(),
            new PhpParquetEngine(),
            new FlowPhpExtension(true, FlowPhpExtension::ABI + 1, '0.46.0'),
        ))->writer(
            memory_filesystem()->writeTo(path('memory://out.parquet')),
            schema(int_schema('id')),
            new PhpBackend(),
        );
    }
}
