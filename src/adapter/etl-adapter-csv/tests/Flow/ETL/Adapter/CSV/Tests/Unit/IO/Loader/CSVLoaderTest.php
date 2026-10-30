<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\Tests\Unit\IO\Loader;

use DateTimeImmutable;
use Flow\ETL\Adapter\CSV\IO\Loader\CSVLoader;
use Flow\ETL\Foundation\Exception\RuntimeException;
use Flow\ETL\Foundation\FlowPhpExtension;
use Flow\ETL\IO\Filesystem\SaveMode;
use Flow\ETL\IO\Loader\File\FileCommit;
use Flow\ETL\IO\Loader\File\Part;
use Flow\ETL\IO\Loader\Partitioning;
use Flow\ETL\Tests\Context\DescribeRoundTrip;
use Flow\ETL\Tests\Context\ReaderRoundTrip;
use Flow\ETL\Tests\Context\StepRoundTrip;
use Flow\ETL\Tests\Double\RecordingFilesystem;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Column\PhpBackend;
use Flow\Filesystem\Path\Option;
use Flow\Filesystem\Path\Option\ContentType;
use Flow\Filesystem\Stream\StringDestinationStream;

use function array_keys;
use function array_map;
use function count;
use function Flow\ETL\Adapter\CSV\to_csv;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\partition_by;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\native_local_filesystem;
use function Flow\Filesystem\DSL\path;

final class CSVLoaderTest extends FlowTestCase
{
    public function test_setting_content_type_on_path(): void
    {
        static::assertEquals(
            (new CSVLoader(path(__DIR__ . '/file.csv')))
                ->destination()
                ->getOption(Option::CONTENT_TYPE),
            ContentType::CSV,
        );
    }

    public function test_every_batch_is_one_append_and_the_header_comes_with_the_first(): void
    {
        $filesystem = new RecordingFilesystem(memory_filesystem());
        $schema = schema(int_schema('id'), str_schema('name'), float_schema('price'), bool_schema('active'));
        $loader = to_csv(path('memory://out.csv'), filesystem: $filesystem)->withNewLineSeparator("\n");

        $loader->load(array_to_rows([[
            'id' => 1,
            'name' => 'a b',
            'price' => 0.1 + 0.2,
            'active' => true,
        ]], $schema), flow_context()->backend());
        $loader->load(array_to_rows([
            ['id' => 2, 'name' => 'c', 'price' => 2.0, 'active' => false],
            ['id' => 3, 'name' => 'd', 'price' => 3.5, 'active' => true],
        ], $schema), flow_context()->backend());
        $loader->closure();

        static::assertSame(
            "id,name,price,active\n1,\"a b\",0.30000000000000004,true\n2,c,2.0,false\n3,d,3.5,true\n",
            $filesystem->readFrom(path('memory://out.csv'))->content(),
        );
        static::assertSame(2, count(array_keys($filesystem->calls, 'append', true)));
    }

    public function test_every_partition_file_gets_its_own_header(): void
    {
        $filesystem = memory_filesystem();
        $schema = schema(int_schema('id'), str_schema('group'));
        $loader = to_csv(path('memory://out/file.csv'), filesystem: $filesystem)
            ->withNewLineSeparator("\n")
            ->partitionBy(partition_by('group'));

        $loader->load(array_to_rows([
            ['id' => 1, 'group' => 'a'],
            ['id' => 2, 'group' => 'b'],
        ], $schema), flow_context()->backend());
        $loader->load(array_to_rows([['id' => 3, 'group' => 'a']], $schema), flow_context()->backend());
        $loader->closure();

        static::assertSame("id\n1\n3\n", $filesystem->readFrom(path('memory://out/group=a/file.csv'))->content());
        static::assertSame("id\n2\n", $filesystem->readFrom(path('memory://out/group=b/file.csv'))->content());
    }

    public function test_the_loader_writes_with_its_options(): void
    {
        $filesystem = memory_filesystem();
        $loader = to_csv(path('memory://out.csv'), filesystem: $filesystem)
            ->withHeader(false)
            ->withSeparator(';')
            ->withEnclosure("'")
            ->withEscape('')
            ->withNewLineSeparator("\r\n")
            ->withDateTimeFormat('Y-m-d H:i');

        $loader->load(
            array_to_rows(
                [['name' => "it's;", 'at' => new DateTimeImmutable('2026-01-02 03:04:05 UTC')]],
                schema(str_schema('name'), datetime_schema('at')),
            ),
            flow_context()->backend(),
        );
        $loader->closure();

        static::assertSame(
            "'it''s;';'2026-01-02 03:04'\r\n",
            $filesystem->readFrom(path('memory://out.csv'))->content(),
        );
    }

    public function test_a_loader_used_again_after_closure_writes_the_header_again(): void
    {
        $filesystem = memory_filesystem();
        $loader = to_csv(path('memory://out.csv'), filesystem: $filesystem)->withNewLineSeparator("\n");

        $loader->load(array_to_rows([['id' => 1]], schema(int_schema('id'))), flow_context()->backend());
        $loader->closure();
        $filesystem->rm(path('memory://out.csv'));
        $loader->load(array_to_rows([['id' => 2]], schema(int_schema('id'))), flow_context()->backend());
        $loader->closure();

        static::assertSame("id\n2\n", $filesystem->readFrom(path('memory://out.csv'))->content());
    }

    public function test_a_discarded_loader_starts_over(): void
    {
        $filesystem = memory_filesystem();
        $loader = to_csv(path('memory://out.csv'), filesystem: $filesystem)->withNewLineSeparator("\n");

        $loader->load(array_to_rows([['id' => 1]], schema(int_schema('id'))), flow_context()->backend());
        $loader->discard();
        $loader->load(array_to_rows([['id' => 2]], schema(int_schema('id'))), flow_context()->backend());
        $loader->closure();

        static::assertSame("id\n2\n", $filesystem->readFrom(path('memory://out.csv'))->content());
    }

    public function test_two_runs_through_one_loader_write_independent_files(): void
    {
        $filesystem = memory_filesystem();
        $loader = to_csv(path('memory://out.csv'), filesystem: $filesystem)
            ->withNewLineSeparator("\n")
            ->partitionBy(partition_by('group'))
            // the second run writes beside the first one's partition, which ExceptionIfExists refuses
            ->saveMode(SaveMode::Append);
        $read = [];

        foreach (['a' => [1, 2], 'b' => [3]] as $group => $ids) {
            $loader->load(
                array_to_rows(
                    array_map(static fn(int $id): array => ['id' => $id, 'group' => $group], $ids),
                    schema(int_schema('id'), str_schema('group')),
                ),
                flow_context()->backend(),
            );
            $loader->closure();
            $read[] = $filesystem->readFrom(path('memory://group=' . $group . '/out.csv'))->content();
        }

        static::assertSame(["id\n1\n2\n", "id\n3\n"], $read);
    }

    public function test_describe_round_trips(): void
    {
        $scratch = ReaderRoundTrip::scratch('csv-describe');
        $file = $scratch->suffix('/rows.csv');
        $loader = to_csv($file, separator: ';', new_line_separator: "\r\n")
            ->saveMode(SaveMode::Overwrite)
            ->withDateFormat('d/m/Y');
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
        $scratch = ReaderRoundTrip::scratch('csv-describe-partitioned');
        static::assertSame(
            ['by' => ['name'], 'write_columns' => true],
            DescribeRoundTrip::rebuilt(
                to_csv($scratch->suffix('/{name}/rows.csv'), separator: ';', new_line_separator: "\r\n")
                    ->saveMode(SaveMode::Overwrite)
                    ->withDateFormat('d/m/Y')
                    ->partitionBy(partition_by('name')->writeColumns()),
            )->describe()?->data['partitioning'],
        );

        native_local_filesystem()->rm($scratch);
    }

    public function test_a_live_filesystem_does_not_describe(): void
    {
        static::assertNull(to_csv(path('memory://rows.csv'), filesystem: memory_filesystem())->describe());
    }

    public function test_in_task_copies_every_setting_and_leaves_the_original(): void
    {
        $scratch = ReaderRoundTrip::scratch('in-task');
        $loader = to_csv($scratch->suffix('/rows.out'), separator: ';')
            ->saveMode(SaveMode::Overwrite)
            ->partitionBy(partition_by('name'))
            ->withDateFormat('d/m/Y');
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
        $filesystem->writeTo(path('memory://out/rows.csv'))->append('old')->close();
        $loader = to_csv(path('memory://out/rows.csv'), filesystem: $filesystem);

        static::assertNull($loader->saveMode(SaveMode::Ignore)->commit('r1', 1));
        static::assertInstanceOf(FileCommit::class, $loader->saveMode(SaveMode::Overwrite)->commit('r1', 1));

        $this->expectException(RuntimeException::class);

        $loader->saveMode(SaveMode::ExceptionIfExists)->commit('r1', 1);
    }

    public function test_partitioning_returns_what_partition_by_set(): void
    {
        $loader = to_csv(path('file:///tmp/rows.csv'), filesystem: native_local_filesystem());

        static::assertEquals(Partitioning::none(), $loader->partitioning());
        static::assertEquals(
            partition_by('name')->writeColumns(),
            $loader->partitionBy(partition_by('name')->writeColumns())->partitioning(),
        );
    }

    public function test_a_writer_writes_with_the_options_of_the_loader(): void
    {
        $stream = new StringDestinationStream(path('memory://out.csv'));

        (new CSVLoader(path('memory://out.csv'), memory_filesystem()))
            ->withSeparator(';')
            ->withNewLineSeparator("\n")
            ->writer($stream, schema(int_schema('id'), str_schema('name')), new PhpBackend())
            ->write(array_to_rows([['id' => 1, 'name' => 'a;b']], schema(int_schema('id'), str_schema('name'))));

        static::assertSame("id;name\n1;\"a;b\"\n", $stream->content());
    }

    public function test_a_writer_without_a_header(): void
    {
        $stream = new StringDestinationStream(path('memory://out.csv'));

        (new CSVLoader(path('memory://out.csv'), memory_filesystem()))
            ->withHeader(false)
            ->withNewLineSeparator("\n")
            ->writer($stream, schema(int_schema('id')), new PhpBackend())
            ->write(array_to_rows([['id' => 1]], schema(int_schema('id'))));

        static::assertSame("1\n", $stream->content());
    }

    public function test_a_flow_php_of_another_abi_is_refused(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('does not match flow-php/etl');

        (new CSVLoader(
            path('memory://out.csv'),
            memory_filesystem(),
            new FlowPhpExtension(true, FlowPhpExtension::ABI + 1, '0.46.0'),
        ))->writer(new StringDestinationStream(path('memory://out.csv')), schema(int_schema('id')), new PhpBackend());
    }

    public function test_in_task_keeps_the_lane_override(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('does not match flow-php/etl');

        (new CSVLoader(
            path('memory://out.csv'),
            memory_filesystem(),
            new FlowPhpExtension(true, FlowPhpExtension::ABI + 1, '0.46.0'),
        ))
            ->inTask(new Part(0, 1, '1a2b3c4d5e6f7a8b', false))
            ->writer(
                new StringDestinationStream(path('memory://other.csv')),
                schema(int_schema('id')),
                new PhpBackend(),
            );
    }

    public function test_without_flow_php_the_php_encoder_writes(): void
    {
        $stream = new StringDestinationStream(path('memory://out.csv'));

        (new CSVLoader(path('memory://out.csv'), memory_filesystem(), new FlowPhpExtension(false, null)))
            ->withHeader(false)
            ->withNewLineSeparator("\n")
            ->writer($stream, schema(int_schema('id')), new PhpBackend())
            ->write(array_to_rows([['id' => 1]], schema(int_schema('id'))));

        static::assertSame("1\n", $stream->content());
    }
}
