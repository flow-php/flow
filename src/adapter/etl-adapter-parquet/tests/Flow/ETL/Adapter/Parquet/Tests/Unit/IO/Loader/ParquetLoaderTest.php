<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Unit\IO\Loader;

use Flow\ETL\Adapter\Parquet\IO\Loader\ParquetLoader;
use Flow\ETL\Adapter\Parquet\Tests\Context\ParquetFilesContext;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Exception\RuntimeException;
use Flow\ETL\IO\Filesystem\SaveMode;
use Flow\ETL\IO\Loader\File\FileCommit;
use Flow\ETL\IO\Loader\File\Part;
use Flow\ETL\IO\Loader\Partitioning;
use Flow\ETL\Tests\Context\DescribeRoundTrip;
use Flow\ETL\Tests\Context\ReaderRoundTrip;
use Flow\ETL\Tests\Context\StepRoundTrip;
use Flow\Filesystem\Path\Option;
use Flow\Filesystem\Path\Option\ContentType;
use Flow\Parquet\Engine\PhpParquetEngine;
use Flow\Parquet\Engine\RustParquetEngine;
use Flow\Parquet\Option as ParquetOption;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetEngine;
use Flow\Parquet\ParquetFile\Compressions;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

use function extension_loaded;
use function Flow\ETL\Adapter\Parquet\to_parquet;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\partition_by;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\native_local_filesystem;
use function Flow\Filesystem\DSL\path;
use function Flow\Types\DSL\type_instance_of;
use function implode;
use function sort;

final class ParquetLoaderTest extends TestCase
{
    public function test_a_second_run_writes_under_its_own_schema(): void
    {
        $context = flow_context(config());
        $memory = memory_filesystem();
        $loader = to_parquet(path('memory://runs/data.parquet'), filesystem: $memory)->saveMode(SaveMode::Append);

        $loader->load(array_to_rows([['id' => 1]], schema(int_schema('id'))), $context->backend());
        $loader->closure();
        $loader->load(array_to_rows([['name' => 'a']], schema(str_schema('name'))), $context->backend());
        $loader->closure();

        $written = [];

        foreach ($memory->list(path('memory://runs/*.parquet')) as $file) {
            $written[] = implode(',', ParquetFilesContext::columnNames($memory, $file->path->uri()));
        }

        sort($written);

        static::assertSame(['id', 'name'], $written);
    }

    public function test_a_batch_without_rows_writes_no_file(): void
    {
        $filesystem = memory_filesystem();
        $loader = to_parquet(path('memory://out.parquet'), filesystem: $filesystem);

        $loader->load(array_to_rows([], schema(int_schema('id'))), flow_context(config())->backend());
        $loader->closure();

        static::assertNull($filesystem->status(path('memory://out.parquet')));
    }

    public function test_setting_content_type_on_path(): void
    {
        static::assertEquals(
            (new ParquetLoader(path(__DIR__ . '/file.parquet')))
                ->destination()
                ->getOption(Option::CONTENT_TYPE),
            ContentType::PARQUET,
        );
    }

    public function test_describe_round_trips(): void
    {
        $scratch = ReaderRoundTrip::scratch('parquet-describe');
        $file = $scratch->suffix('/rows.parquet');
        $loader = to_parquet(
            $file,
            compressions: Compressions::GZIP,
            schema: schema(int_schema('id'), str_schema('name')),
        )->saveMode(SaveMode::Overwrite);
        $rebuilt = DescribeRoundTrip::rebuilt($loader);
        $rows = array_to_rows(
            [['id' => 1, 'name' => 'a'], ['id' => 2, 'name' => 'b'], ['id' => 3, 'name' => 'c']],
            schema(int_schema('id'), str_schema('name')),
        );

        static::assertSame(
            StepRoundTrip::written($loader, $rows, flow_context(), $file),
            StepRoundTrip::written($rebuilt, $rows, flow_context(), $file),
        );

        native_local_filesystem()->rm($scratch);
    }

    public function test_a_partitioned_loader_describes_its_partitioning(): void
    {
        $scratch = ReaderRoundTrip::scratch('parquet-describe-partitioned');
        static::assertSame(
            ['by' => ['name'], 'write_columns' => true],
            DescribeRoundTrip::rebuilt(
                to_parquet(
                    $scratch->suffix('/{name}/rows.parquet'),
                    compressions: Compressions::GZIP,
                    schema: schema(int_schema('id'), str_schema('name')),
                )
                    ->saveMode(SaveMode::Overwrite)
                    ->partitionBy(partition_by('name')->writeColumns()),
            )->describe()?->data['partitioning'],
        );

        native_local_filesystem()->rm($scratch);
    }

    public function test_a_live_filesystem_does_not_describe(): void
    {
        static::assertNull(to_parquet(path('memory://rows.parquet'), filesystem: memory_filesystem())->describe());
    }

    /**
     * @return Generator<string, array{ParquetEngine}>
     */
    public static function engines(): Generator
    {
        yield 'php engine' => [new PhpParquetEngine()];

        if (extension_loaded('arrow')) {
            yield 'rust engine' => [new RustParquetEngine()];
        }
    }

    #[DataProvider('engines')]
    public function test_the_engine_and_its_options_survive_the_round_trip(ParquetEngine $engine): void
    {
        $scratch = ReaderRoundTrip::scratch('parquet-engine-describe');
        $file = $scratch->suffix('/rows.parquet');
        $loader = to_parquet(
            $file,
            options: Options::default()->set(ParquetOption::ROW_GROUP_SIZE_BYTES, 1024),
            engine: $engine,
        )->saveMode(SaveMode::Overwrite);
        $rebuilt = DescribeRoundTrip::rebuilt($loader);
        $rows = array_to_rows(
            [['id' => 1, 'name' => 'a'], ['id' => 2, 'name' => 'b'], ['id' => 3, 'name' => 'c']],
            schema(int_schema('id'), str_schema('name')),
        );

        static::assertSame(
            $engine::class,
            type_instance_of(Description::class)->assert($rebuilt->describe()?->data['engine'])->data['engine'],
        );
        static::assertSame(
            StepRoundTrip::written($loader, $rows, flow_context(), $file),
            StepRoundTrip::written($rebuilt, $rows, flow_context(), $file),
        );

        native_local_filesystem()->rm($scratch);
    }

    public function test_in_task_copies_every_setting_and_leaves_the_original(): void
    {
        $scratch = ReaderRoundTrip::scratch('in-task');
        $loader = to_parquet(
            $scratch->suffix('/rows.out'),
            compressions: Compressions::GZIP,
            schema: schema(int_schema('id')),
            engine: new PhpParquetEngine(),
        )
            ->saveMode(SaveMode::Overwrite)
            ->partitionBy(partition_by('name'));
        $copy = $loader->inTask(new Part(3, 4, '1a2b3c4d5e6f7a8b', false));

        static::assertSame($loader->destination()->uri(), $copy->destination()->uri());
        static::assertSame($scratch->suffix('/rows.out')->uri(), $loader->destination()->uri());
        static::assertEquals($loader->partitioning(), $copy->partitioning());

        $original = $loader->describe()->data ?? [];
        $copied = $copy->describe()->data ?? [];
        static::assertNull($original['task']);
        static::assertSame(
            ['index' => 3, 'splits' => 4, 'run' => '1a2b3c4d5e6f7a8b', 'append' => false],
            $copied['task'],
        );
        static::assertEquals($copied, DescribeRoundTrip::rebuilt($copy)->describe()?->data);
        unset($original['task'], $copied['task']);

        static::assertEquals($original, $copied);

        native_local_filesystem()->rm($scratch);
    }

    public function test_commit_applies_the_save_mode_on_the_loader_filesystem(): void
    {
        $filesystem = memory_filesystem();
        $filesystem->writeTo(path('memory://out/rows.parquet'))->append('old')->close();
        $loader = to_parquet(path('memory://out/rows.parquet'), filesystem: $filesystem);

        static::assertNull($loader->saveMode(SaveMode::Ignore)->commit('r1', 1));
        static::assertInstanceOf(FileCommit::class, $loader->saveMode(SaveMode::Overwrite)->commit('r1', 1));

        $this->expectException(RuntimeException::class);

        $loader->saveMode(SaveMode::ExceptionIfExists)->commit('r1', 1);
    }

    public function test_partitioning_returns_what_partition_by_set(): void
    {
        $loader = to_parquet(path('file:///tmp/rows.parquet'), filesystem: native_local_filesystem());

        static::assertEquals(Partitioning::none(), $loader->partitioning());
        static::assertEquals(
            partition_by('name')->writeColumns(),
            $loader->partitionBy(partition_by('name')->writeColumns())->partitioning(),
        );
    }

    /**
     * @return Generator<string, array{Compressions, list<string>}>
     */
    public static function compression_extensions(): Generator
    {
        yield 'gzip' => [Compressions::GZIP, ['bcmath', 'json', 'zlib']];
        yield 'uncompressed' => [Compressions::UNCOMPRESSED, ['bcmath', 'json', 'zlib']];
        yield 'brotli' => [Compressions::BROTLI, ['bcmath', 'brotli', 'json', 'zlib']];
        yield 'lz4' => [Compressions::LZ4, ['bcmath', 'json', 'lz4', 'zlib']];
        yield 'lz4 raw' => [Compressions::LZ4_RAW, ['bcmath', 'json', 'lz4', 'zlib']];
        yield 'zstd' => [Compressions::ZSTD, ['bcmath', 'json', 'zlib', 'zstd']];
        yield 'snappy' => [
            Compressions::SNAPPY,
            extension_loaded('snappy') ? ['bcmath', 'json', 'snappy', 'zlib'] : ['bcmath', 'json', 'zlib'],
        ];
    }

    /**
     * @param list<string> $expected
     */
    #[DataProvider('compression_extensions')]
    public function test_the_loader_declares_the_parquet_extensions_and_its_codecs(
        Compressions $compressions,
        array $expected,
    ): void {
        static::assertSame(
            $expected,
            to_parquet(path(__DIR__ . '/var/rows.parquet'), compressions: $compressions)->describe()?->extensions(),
        );
    }
}
