<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Text\Tests\Integration\IO\Loader;

use Flow\ETL\Adapter\Text\IO\Loader\TextLoader;
use Flow\ETL\Foundation\Exception\RuntimeException;
use Flow\ETL\IO\Filesystem\SaveMode;
use Flow\ETL\IO\Loader\File\FileCommit;
use Flow\ETL\IO\Loader\File\Part;
use Flow\ETL\IO\Loader\Partitioning;
use Flow\ETL\Tests\Context\DescribeRoundTrip;
use Flow\ETL\Tests\Context\ReaderRoundTrip;
use Flow\ETL\Tests\Context\StepRoundTrip;
use Flow\ETL\Tests\Double\RecordingFilesystem;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Path\Option;
use Flow\Filesystem\Path\Option\ContentType;

use function array_keys;
use function array_map;
use function count;
use function Flow\ETL\Adapter\Text\to_text;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\partition_by;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\native_local_filesystem;
use function Flow\Filesystem\DSL\path;

final class TextLoaderTest extends FlowTestCase
{
    public function test_setting_content_type_on_path(): void
    {
        static::assertEquals(
            (new TextLoader(path(__DIR__ . '/file.txt')))
                ->destination()
                ->getOption(Option::CONTENT_TYPE),
            ContentType::TEXT,
        );
    }

    public function test_every_batch_is_one_append(): void
    {
        $filesystem = new RecordingFilesystem(memory_filesystem());
        $loader = to_text(path('memory://out.txt'), "\n", $filesystem);

        $loader->load(array_to_rows([
            ['price' => 0.1 + 0.2],
            ['price' => 2.0],
        ], schema(float_schema('price'))), flow_context()->backend());
        $loader->load(array_to_rows([['price' => 3.5]], schema(float_schema('price'))), flow_context()->backend());
        $loader->closure();

        static::assertSame(
            "0.30000000000000004\n2.0\n3.5\n",
            $filesystem->readFrom(path('memory://out.txt'))->content(),
        );
        static::assertSame(2, count(array_keys($filesystem->calls, 'append', true)));
    }

    public function test_two_runs_through_one_loader_write_independent_files(): void
    {
        $filesystem = memory_filesystem();
        // the second run writes beside the first one's partition, which ExceptionIfExists refuses
        $loader = to_text(path('memory://out.txt'), filesystem: $filesystem)
            ->partitionBy(partition_by('group'))
            ->saveMode(SaveMode::Append);
        $read = [];

        foreach (['a' => [1, 2], 'b' => [3]] as $group => $ids) {
            $loader->load(
                array_to_rows(
                    array_map(static fn(int $id): array => ['text' => (string) $id, 'group' => $group], $ids),
                    schema(str_schema('text'), str_schema('group')),
                ),
                flow_context()->backend(),
            );
            $loader->closure();
            $read[] = $filesystem->readFrom(path('memory://group=' . $group . '/out.txt'))->content();
        }

        static::assertSame(["1\n2\n", "3\n"], $read);
    }

    public function test_describe_round_trips(): void
    {
        $scratch = ReaderRoundTrip::scratch('text-describe');
        $file = $scratch->suffix('/rows.txt');
        $loader = to_text($file, new_line_separator: "\r\n")->saveMode(SaveMode::Overwrite);
        $rebuilt = DescribeRoundTrip::rebuilt($loader);
        $rows = array_to_rows([['name' => 'a'], ['name' => 'b'], ['name' => 'c']], schema(str_schema('name')));

        static::assertSame(
            StepRoundTrip::written($loader, $rows, flow_context(), $file),
            StepRoundTrip::written($rebuilt, $rows, flow_context(), $file),
        );

        native_local_filesystem()->rm($scratch);
    }

    public function test_a_partitioned_loader_describes_its_partitioning(): void
    {
        $scratch = ReaderRoundTrip::scratch('text-describe-partitioned');
        static::assertSame(
            ['by' => ['name'], 'write_columns' => true],
            DescribeRoundTrip::rebuilt(
                to_text($scratch->suffix('/{name}/rows.txt'), new_line_separator: "\r\n")
                    ->saveMode(SaveMode::Overwrite)
                    ->partitionBy(partition_by('name')->writeColumns()),
            )->describe()?->data['partitioning'],
        );

        native_local_filesystem()->rm($scratch);
    }

    public function test_a_live_filesystem_does_not_describe(): void
    {
        static::assertNull(to_text(path('memory://rows.txt'), filesystem: memory_filesystem())->describe());
    }

    public function test_in_task_copies_every_setting_and_leaves_the_original(): void
    {
        $scratch = ReaderRoundTrip::scratch('in-task');
        $loader = to_text($scratch->suffix('/rows.out'), new_line_separator: "\r\n")
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
        $filesystem->writeTo(path('memory://out/rows.txt'))->append('old')->close();
        $loader = to_text(path('memory://out/rows.txt'), filesystem: $filesystem);

        static::assertNull($loader->saveMode(SaveMode::Ignore)->commit('r1', 1));
        static::assertInstanceOf(FileCommit::class, $loader->saveMode(SaveMode::Overwrite)->commit('r1', 1));

        $this->expectException(RuntimeException::class);

        $loader->saveMode(SaveMode::ExceptionIfExists)->commit('r1', 1);
    }

    public function test_partitioning_returns_what_partition_by_set(): void
    {
        $loader = to_text(path('file:///tmp/rows.txt'), filesystem: native_local_filesystem());

        static::assertEquals(Partitioning::none(), $loader->partitioning());
        static::assertEquals(
            partition_by('name')->writeColumns(),
            $loader->partitionBy(partition_by('name')->writeColumns())->partitioning(),
        );
    }
}
