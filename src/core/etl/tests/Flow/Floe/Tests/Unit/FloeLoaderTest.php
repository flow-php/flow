<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Filesystem\SaveMode;
use Flow\Floe\Exception\IncompatibleSchemaException;
use Flow\Floe\Tests\Context\FloeFilesContext;
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
        $loader->load(array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id'))), $context);
        $loader->closure($context);

        $ids = [];

        foreach (from_floe($path, filesystem: $memory)->extract($context) as $batch) {
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

        $loader->load(array_to_rows([['id' => 1]], schema(int_schema('id'))), $context);
        $loader->closure($context);
        $loader->load(array_to_rows([['name' => 'a']], schema(str_schema('name'))), $context);
        $loader->closure($context);

        $schemas = [];

        foreach ($memory->list(path('memory://runs/*.floe')) as $file) {
            $schemas[] = implode(
                ',',
                FloeFilesContext::phpReader($memory)->read($file->path)->schema()->references()->names(),
            );
        }

        sort($schemas);

        static::assertSame(['id', 'name'], $schemas);
    }

    public function test_destination_returns_path(): void
    {
        $memory = memory_filesystem();
        static::assertSame(
            'memory://out.floe',
            to_floe(path('memory://out.floe'), filesystem: $memory)->destination()->uri(),
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
            $context,
        );
        $loader->closure($context);

        $schema = from_floe($path, filesystem: $memory)->schema();

        static::assertFalse($schema->get('id')->isNullable());
        static::assertTrue($schema->get('note')->isNullable());
    }

    public function test_load_to_path_without_extension_reports_failure(): void
    {
        $this->expectException(RuntimeException::class);

        to_floe(path('memory://no-extension'), filesystem: memory_filesystem())->load(array_to_rows([[
            'id' => 1,
        ]], schema(int_schema('id'))), flow_context(config()));
    }

    public function test_partitioned_batches_write_one_file_per_partition(): void
    {
        $context = flow_context(config());
        $memory = memory_filesystem();
        $base = path('memory://parts/data.floe');

        // one batch carrying both combinations: the loader routes it, B39
        $loader = to_floe($base, filesystem: $memory)->partitionBy(partition_by('country'));
        $loader->load(
            array_to_rows(
                [['id' => 1, 'country' => 'PL'], ['id' => 2, 'country' => 'US']],
                schema(int_schema('id'), str_schema('country')),
            ),
            $context,
        );
        $loader->closure($context);

        static::assertNotNull($memory->status(path('memory://parts/country=PL/data.floe')));
        static::assertNotNull($memory->status(path('memory://parts/country=US/data.floe')));

        $ids = [];

        foreach (from_floe(path('memory://parts/**/*.floe'), filesystem: $memory)->extract($context) as $batch) {
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
        $loader->load(array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id'))), $context);
        $loader->load(array_to_rows([['id' => 3]], schema(int_schema('id'))), $context);
        $loader->closure($context);

        $ids = [];

        // reading the concrete (non-pattern) path proves all rows landed in one file
        foreach (from_floe($path, filesystem: $memory)->extract($context) as $batch) {
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
}
