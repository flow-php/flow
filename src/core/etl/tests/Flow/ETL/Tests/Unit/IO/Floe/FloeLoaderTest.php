<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\IO\Floe;

use Flow\ETL\Foundation\Exception\RuntimeException;
use Flow\ETL\IO\Extractor\Criteria;
use Flow\ETL\IO\Filesystem\SaveMode;
use Flow\ETL\IO\Floe\FloeLoader;
use Flow\ETL\IO\Loader\File\FileCommit;
use Flow\ETL\IO\Loader\File\Part;
use Flow\ETL\IO\Loader\Partitioning;
use Flow\ETL\Storage\Floe\Exception\IncompatibleSchemaException;
use Flow\ETL\Storage\Floe\Options;
use Flow\ETL\Tests\Context\DescribeRoundTrip;
use Flow\ETL\Tests\Context\FloeFilesContext;
use Flow\ETL\Tests\Context\ReaderRoundTrip;
use Flow\ETL\Tests\Context\StepRoundTrip;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Column\Frame\Codec;
use Flow\ETL\Values\Schema\Metadata;
use PHPUnit\Framework\TestCase;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\partition_by;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\native_local_filesystem;
use function Flow\Filesystem\DSL\path;
use function Flow\Floe\DSL\from_floe;
use function Flow\Floe\DSL\to_floe;
use function implode;
use function sort;

final class FloeLoaderTest extends TestCase
{
    public function test_closure_writes_a_readable_non_torn_file(): void
    {
        $context = flow_context(config());
        $memory = memory_filesystem();
        $path = path('memory://closed.floe');

        $loader = to_floe($path, filesystem: $memory);
        $loader->load(array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id'))), $context->backend());
        $loader->closure();

        $ids = [];

        foreach (FlowTestCase::extracted(
            from_floe($path, filesystem: $memory),
            $context->backend(),
            new Criteria(),
        ) as $batch) {
            foreach ($batch->toArray() as $extractedRow) {
                $ids[] = $extractedRow['id'];
            }
        }

        static::assertSame([1, 2], $ids);
    }

    public function test_a_second_run_writes_under_its_own_schema(): void
    {
        $context = flow_context(config());
        $memory = memory_filesystem();
        $loader = to_floe(path('memory://runs/data.floe'), filesystem: $memory)->saveMode(SaveMode::Append);

        $loader->load(array_to_rows([['id' => 1]], schema(int_schema('id'))), $context->backend());
        $loader->closure();
        $loader->load(array_to_rows([['name' => 'a']], schema(str_schema('name'))), $context->backend());
        $loader->closure();

        $schemas = [];

        foreach ($memory->list(path('memory://runs/*.floe')) as $file) {
            $schemas[] = implode(',', FloeFilesContext::files($memory)->read($file->path)->schema()->names());
        }

        sort($schemas);

        static::assertSame(['id', 'name'], $schemas);
    }

    public function test_a_batch_without_rows_writes_no_file(): void
    {
        $memory = memory_filesystem();
        $loader = to_floe(path('memory://out.floe'), filesystem: $memory);

        $loader->load(array_to_rows([], schema(int_schema('id'))), flow_context(config())->backend());
        $loader->closure();

        static::assertNull($memory->status(path('memory://out.floe')));
    }

    public function test_destination_returns_path(): void
    {
        static::assertSame(
            'memory://out.floe',
            to_floe(path('memory://out.floe'), filesystem: memory_filesystem())->destination()->uri(),
        );
    }

    public function test_inferred_schema_preserves_nullability(): void
    {
        $context = flow_context(config());
        $memory = memory_filesystem();
        $path = path('memory://nullability.floe');

        $loader = to_floe($path, filesystem: $memory);
        $loader->load(
            array_to_rows(
                [['id' => 1, 'note' => 'a'], ['id' => 2, 'note' => null]],
                schema(int_schema('id'), str_schema('note', nullable: true)),
            ),
            $context->backend(),
        );
        $loader->closure();

        $schema = from_floe($path, filesystem: $memory)->schema();

        static::assertFalse($schema->get('id')->isNullable());
        static::assertTrue($schema->get('note')->isNullable());
    }

    public function test_load_to_path_without_extension_reports_failure(): void
    {
        $this->expectException(RuntimeException::class);

        to_floe(path('memory://no-extension'), filesystem: memory_filesystem())->load(array_to_rows([[
            'id' => 1,
        ]], schema(int_schema('id'))), flow_context(config())->backend());
    }

    public function test_partitioned_batches_write_one_file_per_partition(): void
    {
        $context = flow_context(config());
        $memory = memory_filesystem();
        // one batch carrying both combinations: the loader routes it, B39
        $loader = to_floe(path('memory://parts/data.floe'), filesystem: $memory)->partitionBy(partition_by('country'));
        $loader->load(
            array_to_rows(
                [['id' => 1, 'country' => 'PL'], ['id' => 2, 'country' => 'US']],
                schema(int_schema('id'), str_schema('country')),
            ),
            $context->backend(),
        );
        $loader->closure();

        static::assertNotNull($memory->status(path('memory://parts/country=PL/data.floe')));
        static::assertNotNull($memory->status(path('memory://parts/country=US/data.floe')));

        $ids = [];

        foreach (FlowTestCase::extracted(
            from_floe(path('memory://parts/**/*.floe'), filesystem: $memory),
            $context->backend(),
            new Criteria(),
        ) as $batch) {
            foreach ($batch->toArray() as $extractedRow) {
                $ids[] = $extractedRow['id'];
            }
        }

        sort($ids);

        static::assertSame([1, 2], $ids);
    }

    public function test_repeated_loads_write_a_single_file(): void
    {
        $context = flow_context(config());
        $memory = memory_filesystem();
        $path = path('memory://repeated.floe');

        $loader = to_floe($path, filesystem: $memory);
        $loader->load(array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id'))), $context->backend());
        $loader->load(array_to_rows([['id' => 3]], schema(int_schema('id'))), $context->backend());
        $loader->closure();

        $ids = [];

        // reading the concrete (non-pattern) path proves all rows landed in one file
        foreach (FlowTestCase::extracted(
            from_floe($path, filesystem: $memory),
            $context->backend(),
            new Criteria(),
        ) as $batch) {
            foreach ($batch->toArray() as $extractedRow) {
                $ids[] = $extractedRow['id'];
            }
        }

        static::assertSame([1, 2, 3], $ids);
    }

    /**
     * The loader strips only what partitionBy() moved into the path. A declared column the rows do
     * not carry stays declared, and refusing it is the writer's job.
     */
    public function test_a_declared_column_the_rows_do_not_carry_is_refused_not_dropped(): void
    {
        $filesystem = memory_filesystem();

        $this->expectException(IncompatibleSchemaException::class);
        $this->expectExceptionMessage('Missing Definitions');

        data_frame()
            ->read(from_array([['id' => 1, 'name' => 'a']]))
            ->write(to_floe(path('memory://declared-missing.floe'), filesystem: $filesystem)->withSchema(schema(
                int_schema('id'),
                str_schema('name'),
                str_schema('missing_col'),
            )))
            ->run();
    }

    public function test_describe_round_trips(): void
    {
        $scratch = ReaderRoundTrip::scratch('floe-describe');
        $file = $scratch->suffix('/rows.floe');
        $loader = (new FloeLoader($file, Metadata::fromArray(['owner' => 'tests']), new Options(1024)))
            ->saveMode(SaveMode::Overwrite)
            ->withSchema(schema(int_schema('id'), str_schema('name')));
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
        $scratch = ReaderRoundTrip::scratch('floe-describe-partitioned');
        static::assertSame(
            ['by' => ['name'], 'write_columns' => true],
            DescribeRoundTrip::rebuilt(
                (new FloeLoader(
                    $scratch->suffix('/{name}/rows.floe'),
                    Metadata::fromArray(['owner' => 'tests']),
                    new Options(1024),
                ))
                    ->saveMode(SaveMode::Overwrite)
                    ->withSchema(schema(int_schema('id'), str_schema('name')))
                    ->partitionBy(partition_by('name')->writeColumns()),
            )->describe()?->data['partitioning'],
        );

        native_local_filesystem()->rm($scratch);
    }

    public function test_a_live_filesystem_does_not_describe(): void
    {
        static::assertNull(to_floe(path('memory://rows.floe'), filesystem: memory_filesystem())->describe());
    }

    public function test_a_loader_with_another_codec_does_not_describe(): void
    {
        static::assertNull(
            (new FloeLoader(
                ($scratch = ReaderRoundTrip::scratch('floe-codec'))->suffix('/rows.floe'),
                options: new Options(1024, new class implements Codec {
                    public function encode(string $data): string
                    {
                        return $data;
                    }

                    public function decode(string $data): string
                    {
                        return $data;
                    }

                    public function id(): int
                    {
                        return 7;
                    }
                }),
            ))->describe(),
        );

        native_local_filesystem()->rm($scratch);
    }

    public function test_in_task_copies_every_setting_and_leaves_the_original(): void
    {
        $scratch = ReaderRoundTrip::scratch('in-task');
        $loader = (new FloeLoader(
            $scratch->suffix('/rows.out'),
            Metadata::fromArray(['owner' => 'tests']),
            new Options(1024),
        ))
            ->saveMode(SaveMode::Overwrite)
            ->partitionBy(partition_by('name'))
            ->withSchema(schema(int_schema('id')));
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
        $filesystem->writeTo(path('memory://out/rows.floe'))->append('old')->close();
        $loader = to_floe(path('memory://out/rows.floe'), filesystem: $filesystem);

        static::assertNull($loader->saveMode(SaveMode::Ignore)->commit('r1', 1));
        static::assertInstanceOf(FileCommit::class, $loader->saveMode(SaveMode::Overwrite)->commit('r1', 1));

        $this->expectException(RuntimeException::class);

        $loader->saveMode(SaveMode::ExceptionIfExists)->commit('r1', 1);
    }

    public function test_partitioning_returns_what_partition_by_set(): void
    {
        $loader = to_floe(path('file:///tmp/rows.floe'), filesystem: native_local_filesystem());

        static::assertEquals(Partitioning::none(), $loader->partitioning());
        static::assertEquals(
            partition_by('name')->writeColumns(),
            $loader->partitionBy(partition_by('name')->writeColumns())->partitioning(),
        );
    }
}
