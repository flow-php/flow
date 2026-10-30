<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\IO\Loader\File;

use Flow\ETL\Foundation\Exception\RuntimeException as FlowRuntimeException;
use Flow\ETL\IO\Filesystem\SaveMode;
use Flow\ETL\IO\Loader\File\FileWrite;
use Flow\ETL\IO\Loader\File\Part;
use Flow\ETL\IO\Loader\File\PartitionRouter;
use Flow\ETL\IO\Loader\Partitioning;
use Flow\ETL\Tests\Context\ReaderRoundTrip;
use Flow\ETL\Tests\Context\WrittenFiles;
use Flow\ETL\Tests\Double\SpyFileWriterFactory;
use Flow\ETL\Tests\FlowTestCase;
use PHPUnit\Framework\Attributes\TestWith;
use RuntimeException;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\partition_by;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\native_local_filesystem;
use function Flow\Filesystem\DSL\path;

final class FileWriteTest extends FlowTestCase
{
    public function test_a_task_stages_its_files_and_its_closure_commits_nothing(): void
    {
        $out = ReaderRoundTrip::scratch('frame-task');
        $frame = new FileWrite(
            native_local_filesystem(),
            $out->suffix('/part-{part}.txt'),
            SaveMode::Overwrite,
            new PartitionRouter(Partitioning::none()),
            new SpyFileWriterFactory(),
            new Part(1, 2, 'r1', false),
        );

        $frame->write(array_to_rows([['id' => 1]], schema(int_schema('id'))), flow_context()->backend());
        $frame->closure();

        static::assertMatchesRegularExpression(
            '#^\.flow-tmp-r1-[0-9a-f]{8}/00001/part-00001\.txt$#',
            WrittenFiles::under($out)[0] ?? '',
        );
        static::assertCount(1, WrittenFiles::under($out));

        native_local_filesystem()->rm($out);
    }

    public function test_a_task_discard_removes_only_its_own_files(): void
    {
        $out = ReaderRoundTrip::scratch('frame-task-discard');
        $frames = [];

        foreach ([0, 1] as $index) {
            $frames[$index] = new FileWrite(
                native_local_filesystem(),
                $out->suffix('/part-{part}.txt'),
                SaveMode::Overwrite,
                new PartitionRouter(Partitioning::none()),
                new SpyFileWriterFactory(),
                new Part($index, 2, 'r1', false),
            );
            $frames[$index]->write(array_to_rows([['id' => 1]], schema(int_schema('id'))), flow_context()->backend());
        }

        $frames[1]->discard();
        $frames[0]->closure();

        static::assertMatchesRegularExpression(
            '#^\.flow-tmp-r1-[0-9a-f]{8}/00000/part-00000\.txt$#',
            WrittenFiles::under($out)[0] ?? '',
        );
        static::assertCount(1, WrittenFiles::under($out));

        native_local_filesystem()->rm($out);
    }

    public function test_a_closure_without_rows_under_overwrite_empties_an_unpartitioned_destination(): void
    {
        $out = ReaderRoundTrip::scratch('frame-empty-overwrite');
        native_local_filesystem()->writeTo($out->suffix('/part-00007.txt'))->append('old')->close();

        (new FileWrite(
            native_local_filesystem(),
            $out->suffix('/part-{part}.txt'),
            SaveMode::Overwrite,
            new PartitionRouter(Partitioning::none()),
            new SpyFileWriterFactory(),
        ))->closure();

        static::assertSame([], WrittenFiles::under($out));

        native_local_filesystem()->rm($out);
    }

    public function test_a_closure_without_rows_under_overwrite_keeps_every_partition(): void
    {
        $out = ReaderRoundTrip::scratch('frame-empty-overwrite-partitioned');
        native_local_filesystem()->writeTo($out->suffix('/group=a/part-00007.txt'))->append('old')->close();

        (new FileWrite(
            native_local_filesystem(),
            $out->suffix('/part-{part}.txt'),
            SaveMode::Overwrite,
            new PartitionRouter(partition_by('group')),
            new SpyFileWriterFactory(),
        ))->closure();

        static::assertSame(['group=a/part-00007.txt'], WrittenFiles::under($out));

        native_local_filesystem()->rm($out);
    }

    #[TestWith([SaveMode::Append])]
    #[TestWith([SaveMode::Ignore])]
    public function test_a_closure_without_rows_keeps_the_destination(SaveMode $mode): void
    {
        $out = ReaderRoundTrip::scratch('frame-empty-' . $mode->name);
        native_local_filesystem()->writeTo($out->suffix('/part-00007.txt'))->append('old')->close();

        (new FileWrite(
            native_local_filesystem(),
            $out->suffix('/part-{part}.txt'),
            $mode,
            new PartitionRouter(Partitioning::none()),
            new SpyFileWriterFactory(),
        ))->closure();

        static::assertSame(['part-00007.txt'], WrittenFiles::under($out));

        native_local_filesystem()->rm($out);
    }

    public function test_a_write_without_rows_under_exception_if_exists_refuses_an_existing_destination(): void
    {
        $filesystem = memory_filesystem();
        $filesystem->writeTo(path('memory://out/part-00007.txt'))->append('old')->close();

        $this->expectException(FlowRuntimeException::class);
        $this->expectExceptionMessage('already exists');

        new FileWrite(
            $filesystem,
            path('memory://out/part-{part}.txt'),
            SaveMode::ExceptionIfExists,
            new PartitionRouter(Partitioning::none()),
            new SpyFileWriterFactory(),
        );
    }

    public function test_one_writer_per_partition_uri(): void
    {
        $filesystem = memory_filesystem();
        $writers = new SpyFileWriterFactory();
        $frame = new FileWrite(
            $filesystem,
            path('memory://out/file.txt'),
            SaveMode::ExceptionIfExists,
            new PartitionRouter(partition_by('group')),
            $writers,
        );
        $schema = schema(int_schema('id'), str_schema('group'));

        $frame->write(array_to_rows([
            ['id' => 1, 'group' => 'a'],
            ['id' => 2, 'group' => 'b'],
        ], $schema), flow_context()->backend());
        $frame->write(array_to_rows([['id' => 3, 'group' => 'a']], $schema), flow_context()->backend());
        $frame->closure();

        static::assertSame(['memory://out/group=a/file.txt', 'memory://out/group=b/file.txt'], $writers->opened);
        static::assertSame(
            'rows:1;rows:1;closed;',
            $filesystem->readFrom(path('memory://out/group=a/file.txt'))->content(),
        );
        static::assertSame('rows:1;closed;', $filesystem->readFrom(path('memory://out/group=b/file.txt'))->content());
    }

    public function test_a_batch_without_rows_opens_no_writer_and_publishes_no_file(): void
    {
        $filesystem = memory_filesystem();
        $writers = new SpyFileWriterFactory();
        $frame = new FileWrite(
            $filesystem,
            path('memory://out/file.txt'),
            SaveMode::ExceptionIfExists,
            new PartitionRouter(partition_by('group')),
            $writers,
        );

        $frame->write(array_to_rows([], schema(int_schema('id'), str_schema('group'))), flow_context()->backend());
        $frame->closure();

        static::assertSame([], $writers->opened);
        static::assertSame([], iterator_to_array($filesystem->list(path('memory://out/**/*'))));
    }

    public function test_closure_closes_every_writer_before_it_publishes(): void
    {
        $filesystem = memory_filesystem();
        $frame = new FileWrite(
            $filesystem,
            path('memory://out/file.txt'),
            SaveMode::ExceptionIfExists,
            new PartitionRouter(Partitioning::none()),
            new SpyFileWriterFactory(),
        );

        $frame->write(array_to_rows([['id' => 1]], schema(int_schema('id'))), flow_context()->backend());
        $frame->closure();

        static::assertSame('rows:1;closed;', $filesystem->readFrom(path('memory://out/file.txt'))->content());
    }

    public function test_discard_closes_every_writer_and_leaves_no_file(): void
    {
        $filesystem = memory_filesystem();
        $writers = new SpyFileWriterFactory();
        $frame = new FileWrite(
            $filesystem,
            path('memory://out/file.txt'),
            SaveMode::ExceptionIfExists,
            new PartitionRouter(Partitioning::none()),
            $writers,
        );

        $frame->write(array_to_rows([['id' => 1]], schema(int_schema('id'))), flow_context()->backend());
        $frame->discard();

        static::assertSame(['memory://out/file.txt'], $writers->closed);
        static::assertNull($filesystem->status(path('memory://out/file.txt')));
    }

    public function test_closure_closes_every_writer_then_rethrows_the_first_close_failure(): void
    {
        $writers = new SpyFileWriterFactory(failingClose: true);
        $frame = new FileWrite(
            memory_filesystem(),
            path('memory://out/file.txt'),
            SaveMode::ExceptionIfExists,
            new PartitionRouter(partition_by('group')),
            $writers,
        );

        $frame->write(
            array_to_rows(
                [['id' => 1, 'group' => 'a'], ['id' => 2, 'group' => 'b']],
                schema(int_schema('id'), str_schema('group')),
            ),
            flow_context()->backend(),
        );

        try {
            $frame->closure();
            static::fail('a failing close must reach the caller');
        } catch (RuntimeException $e) {
            static::assertSame('close of memory://out/group=a/file.txt failed', $e->getMessage());
        }

        static::assertSame(['memory://out/group=a/file.txt', 'memory://out/group=b/file.txt'], $writers->closed);
    }
}
