<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\IO\Loader\File;

use Flow\ETL\Foundation\Exception\RuntimeException;
use Flow\ETL\IO\Filesystem\SaveMode;
use Flow\ETL\IO\Loader\File\FileCommit;
use Flow\ETL\Tests\Context\ReaderRoundTrip;
use Flow\ETL\Tests\Context\WrittenFiles;
use Flow\ETL\Tests\Double\RecordingFilesystem;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Exception\RuntimeException as FilesystemRuntimeException;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Partition;
use Flow\Filesystem\Path;
use Flow\Filesystem\Tests\Double\FailingMvFilesystem;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function array_search;
use function Flow\ETL\DSL\append;
use function Flow\ETL\DSL\exception_if_exists;
use function Flow\ETL\DSL\ignore;
use function Flow\ETL\DSL\overwrite;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\native_local_filesystem;
use function Flow\Filesystem\DSL\path;
use function glob;
use function range;
use function sort;
use function sprintf;

final class FileCommitTest extends FlowTestCase
{
    public Path $out;

    protected function setUp(): void
    {
        parent::setUp();
        $this->out = ReaderRoundTrip::scratch('file-commit');
    }

    protected function tearDown(): void
    {
        native_local_filesystem()->rm($this->out);
        parent::tearDown();
    }

    /**
     * @return Generator<string, array{SaveMode}>
     */
    public static function modes(): Generator
    {
        yield 'append' => [append()];
        yield 'ignore' => [ignore()];
        yield 'exception if exists' => [exception_if_exists()];
        yield 'overwrite' => [overwrite()];
    }

    /**
     * @return Generator<string, array{SaveMode}>
     */
    public static function modes_that_keep_parts(): Generator
    {
        yield 'append' => [append()];
        yield 'ignore' => [ignore()];
        yield 'exception if exists' => [exception_if_exists()];
    }

    /**
     * @return list<string>
     */
    public static function written(Filesystem $filesystem, string $glob): array
    {
        $uris = [];

        foreach ($filesystem->list(path($glob)) as $status) {
            if ($status->isFile()) {
                $uris[] = $status->path->uri();
            }
        }

        sort($uris);

        return $uris;
    }

    public function test_abort_closes_every_stream_before_removing_any_file(): void
    {
        $filesystem = new RecordingFilesystem(memory_filesystem());
        $commit = new FileCommit($filesystem, path('memory://out.csv'), exception_if_exists(), 'r1');
        $commit->open()->append('half written');

        $commit->abort();

        // a format writer flushes its footer on close, so the file must not be removed before that happens
        static::assertLessThan(
            array_search('rm', $filesystem->calls, true),
            array_search('close', $filesystem->calls, true),
        );
    }

    public function test_a_destination_without_an_extension_is_refused(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Stream path must have an extension');

        new FileCommit(memory_filesystem(), path('memory://out'), exception_if_exists(), 'r1');
    }

    public function test_a_pattern_destination_is_refused(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage("Destination path can't be pattern");

        new FileCommit(memory_filesystem(), path('memory://out/*.csv'), exception_if_exists(), 'r1');
    }

    public function test_a_placeholder_destination_without_partitions_is_refused(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('contains partition placeholders but rows are not partitioned');

        (new FileCommit(memory_filesystem(), path('memory://out/{name}.csv'), exception_if_exists(), 'r1'))->open();
    }

    public function test_the_same_destination_returns_the_same_stream(): void
    {
        $commit = new FileCommit(memory_filesystem(), path('memory://out.csv'), exception_if_exists(), 'r1');

        static::assertSame($commit->open(), $commit->open());
    }

    public function test_a_serial_write_fills_part_zero_in_place(): void
    {
        $commit = new FileCommit(
            memory_filesystem(),
            path('memory://out/part-{part}.csv'),
            exception_if_exists(),
            'r1',
        );

        static::assertSame('memory://out/part-00000.csv', $commit->open()->path()->uri());
    }

    public function test_append_fills_part_zero_with_the_run(): void
    {
        $commit = new FileCommit(memory_filesystem(), path('memory://out/part-{part}.csv'), append(), 'r1');

        static::assertSame('memory://out/part-00000-r1.csv', $commit->open()->path()->uri());
    }

    public function test_append_over_an_existing_plain_file_names_the_run_after_a_dash(): void
    {
        $filesystem = memory_filesystem();
        $filesystem->writeTo(path('memory://out/orders.csv'))->append('old')->close();

        static::assertSame(
            'memory://out/orders-r1.csv',
            (new FileCommit($filesystem, path('memory://out/orders.csv'), append(), 'r1'))->open()->path()->uri(),
        );
    }

    public function test_an_overwrite_stages_inside_the_destination_directory(): void
    {
        $commit = new FileCommit(native_local_filesystem(), $this->out->suffix('/orders.csv'), overwrite(), 'r1');

        static::assertMatchesRegularExpression(
            '#^' . $this->out->uri() . '/\.flow-tmp-r1-[0-9a-f]{8}/00000/orders\.csv$#',
            $commit->open()->path()->uri(),
        );
    }

    public function test_a_task_stages_under_the_static_part_of_a_pattern(): void
    {
        $destination = $this->out->suffix('/orders.csv/part-{part}.csv');
        $job = new FileCommit(native_local_filesystem(), $destination, exception_if_exists(), 'r1', 4);
        $task = new FileCommit(native_local_filesystem(), $destination, exception_if_exists(), 'r1', 4, $job->task(2));

        static::assertMatchesRegularExpression(
            '#^' . $this->out->uri() . '/orders\.csv/\.flow-tmp-r1-[0-9a-f]{8}/00002/dept=a/part-00002\.csv$#',
            $task->open([new Partition('dept', 'a')])->path()->uri(),
        );
    }

    public function test_exception_if_exists_refuses_an_existing_plain_file(): void
    {
        $filesystem = memory_filesystem();
        $filesystem->writeTo(path('memory://out/orders.csv'))->append('old')->close();

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Destination path "memory://out/orders.csv" already exists');

        (new FileCommit($filesystem, path('memory://out/orders.csv'), exception_if_exists(), 'r1'))->admit(false);
    }

    public function test_ignore_does_not_admit_an_existing_plain_file(): void
    {
        $filesystem = memory_filesystem();
        $filesystem->writeTo(path('memory://out/orders.csv'))->append('old')->close();

        static::assertFalse((new FileCommit($filesystem, path('memory://out/orders.csv'), ignore(), 'r1'))->admit(
            false,
        ));
    }

    public function test_a_partitioned_plain_destination_is_decided_once_over_every_partition(): void
    {
        $filesystem = memory_filesystem();
        $filesystem->writeTo(path('memory://out/dept=b/orders.csv'))->append('old')->close();

        static::assertFalse((new FileCommit($filesystem, path('memory://out/orders.csv'), ignore(), 'r1'))->admit(
            true,
        ));

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Destination path "memory://out/**/orders.csv" already exists');

        (new FileCommit($filesystem, path('memory://out/orders.csv'), exception_if_exists(), 'r1'))->admit(true);
    }

    public function test_admit_answers_true_for_a_fresh_destination_and_for_overwrite_and_append(): void
    {
        $filesystem = memory_filesystem();

        static::assertTrue((new FileCommit($filesystem, path('memory://out/part-{part}.csv'), ignore(), 'r1'))->admit(
            false,
        ));

        $filesystem->writeTo(path('memory://out/part-00003.csv'))->append('x')->close();

        static::assertTrue((new FileCommit(
            $filesystem,
            path('memory://out/part-{part}.csv'),
            overwrite(),
            'r1',
        ))->admit(false));
        static::assertTrue((new FileCommit($filesystem, path('memory://out/part-{part}.csv'), append(), 'r1'))->admit(
            false,
        ));
    }

    public function test_admit_answers_false_for_ignore_over_an_existing_part(): void
    {
        $filesystem = memory_filesystem();
        $filesystem->writeTo(path('memory://out/part-00003.csv'))->append('x')->close();

        static::assertFalse((new FileCommit($filesystem, path('memory://out/part-{part}.csv'), ignore(), 'r1'))->admit(
            false,
        ));
    }

    public function test_admit_globs_partition_directories_when_partitioned(): void
    {
        $filesystem = memory_filesystem();
        $filesystem->writeTo(path('memory://out/dept=a/part-00000.csv'))->append('x')->close();

        static::assertTrue((new FileCommit($filesystem, path('memory://out/part-{part}.csv'), ignore(), 'r1'))->admit(
            false,
        ));
        static::assertFalse((new FileCommit($filesystem, path('memory://out/part-{part}.csv'), ignore(), 'r1'))->admit(
            true,
        ));
    }

    public function test_admit_refuses_exception_if_exists_over_an_existing_part(): void
    {
        $filesystem = memory_filesystem();
        $filesystem->writeTo(path('memory://out/part-00003.csv'))->append('x')->close();

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage(
            'Destination path "memory://out/part-*.csv" already exists, please change path to different or set different SaveMode',
        );

        (new FileCommit($filesystem, path('memory://out/part-{part}.csv'), exception_if_exists(), 'r1'))->admit(false);
    }

    public function test_admit_ignores_another_jobs_temporary_output(): void
    {
        $filesystem = memory_filesystem();
        $filesystem->writeTo(path('memory://out/.flow-tmp-r0-abcd/00000/part-00000.csv'))->append('x')->close();

        static::assertTrue((new FileCommit(
            $filesystem,
            path('memory://out/part-{part}.csv'),
            exception_if_exists(),
            'r1',
        ))->admit(true));
    }

    public function test_append_refuses_a_file_at_the_static_root(): void
    {
        $filesystem = memory_filesystem();
        $filesystem->writeTo(path('memory://out'))->append('a file')->close();

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('is a file');

        (new FileCommit($filesystem, path('memory://out/orders-{part}.csv'), append(), 'r1'))->admit(false);
    }

    public function test_the_task_names_its_part_with_the_run_under_append(): void
    {
        static::assertSame(
            '00001-r1',
            (new FileCommit(memory_filesystem(), path('memory://out/{part}.csv'), append(), 'r1', 4))
                ->task(1)
                ->name(),
        );
        static::assertSame(
            '00001',
            (new FileCommit(memory_filesystem(), path('memory://out/{part}.csv'), overwrite(), 'r1', 4))
                ->task(1)
                ->name(),
        );
    }

    public function test_overwrite_removes_every_stale_part(): void
    {
        $filesystem = native_local_filesystem();

        foreach (['00000', '00001', '00002'] as $part) {
            $filesystem->writeTo($this->out->suffix("/part-{$part}.csv"))->append('stale')->close();
        }

        $commit = new FileCommit($filesystem, $this->out->suffix('/part-{part}.csv'), overwrite(), 'r1');
        $commit->open()->append('fresh');
        $commit->commit([0]);

        static::assertSame(['part-00000.csv'], WrittenFiles::under($this->out));
        static::assertSame('fresh', $filesystem->readFrom($this->out->suffix('/part-00000.csv'))->content());
    }

    public function test_overwrite_replaces_a_plain_file_and_its_appended_siblings(): void
    {
        $filesystem = native_local_filesystem();

        foreach (['orders.csv', 'orders-0123456789abcdef.csv', 'orders_backup.csv'] as $file) {
            $filesystem
                ->writeTo($this->out->suffix('/' . $file))
                ->append('stale')
                ->close();
        }

        $commit = new FileCommit($filesystem, $this->out->suffix('/orders.csv'), overwrite(), 'r1');
        $commit->open()->append('fresh');
        $commit->commit([0]);

        static::assertSame(['orders.csv', 'orders_backup.csv'], WrittenFiles::under($this->out));
        static::assertSame('fresh', $filesystem->readFrom($this->out->suffix('/orders.csv'))->content());
    }

    public function test_overwrite_removes_stale_parts_of_a_bigger_run(): void
    {
        $filesystem = native_local_filesystem();
        $destination = $this->out->suffix('/part-{part}.csv');

        foreach (range(0, 5) as $index) {
            $filesystem
                ->writeTo($this->out->suffix(sprintf('/part-%05d.csv', $index)))
                ->append('old')
                ->close();
        }

        $job = new FileCommit($filesystem, $destination, overwrite(), 'r1', 3);

        foreach (range(0, 2) as $index) {
            (new FileCommit($filesystem, $destination, overwrite(), 'r1', 3, $job->task($index)))
                ->open()
                ->append('new')
                ->close();
        }

        $job->commit([0, 1, 2]);

        static::assertSame(['part-00000.csv', 'part-00001.csv', 'part-00002.csv'], WrittenFiles::under($this->out));
        static::assertSame([], WrittenFiles::temporaryOutputs($this->out));
    }

    public function test_overwrite_with_partitions_removes_only_written_partitions(): void
    {
        $filesystem = native_local_filesystem();
        $destination = $this->out->suffix('/part-{part}.csv');

        foreach (['a', 'b'] as $dept) {
            foreach (['00000', '00001'] as $part) {
                $filesystem->writeTo($this->out->suffix("/dept={$dept}/part-{$part}.csv"))->append('stale')->close();
            }
        }

        $job = new FileCommit($filesystem, $destination, overwrite(), 'r1', 2);
        $job->admit(true);
        (new FileCommit($filesystem, $destination, overwrite(), 'r1', 2, $job->task(1)))
            ->open([new Partition('dept', 'a')])
            ->append('fresh')
            ->close();
        $job->commit([1]);

        static::assertSame(
            ['dept=a/part-00001.csv', 'dept=b/part-00000.csv', 'dept=b/part-00001.csv'],
            WrittenFiles::under($this->out),
        );
    }

    public function test_overwrite_into_a_placeholder_destination_removes_only_written_partitions(): void
    {
        $filesystem = native_local_filesystem();
        $destination = $this->out->suffix('/{dept}/part-{part}.csv');

        foreach (['a', 'b'] as $dept) {
            $filesystem->writeTo($this->out->suffix("/{$dept}/part-00000.csv"))->append('old')->close();
        }

        $job = new FileCommit($filesystem, $destination, overwrite(), 'r1', 2);
        $job->admit(true);
        (new FileCommit($filesystem, $destination, overwrite(), 'r1', 2, $job->task(1)))
            ->open([new Partition('dept', 'a')])
            ->append('fresh')
            ->close();
        $job->commit([1]);

        static::assertSame(['a/part-00001.csv', 'b/part-00000.csv'], WrittenFiles::under($this->out));
    }

    public function test_overwrite_with_partitions_and_no_part_removes_only_files_flow_named(): void
    {
        $filesystem = native_local_filesystem();

        foreach ([
            '/dept=a/orders.csv',
            '/dept=a/orders-0123456789abcdef.csv',
            '/dept=a/orders_backup.csv',
            '/dept=a/other.csv',
            '/dept=b/orders.csv',
        ] as $file) {
            $filesystem->writeTo($this->out->suffix($file))->append('stale')->close();
        }

        $commit = new FileCommit($filesystem, $this->out->suffix('/orders.csv'), overwrite(), 'r1');
        $commit->admit(true);
        $commit
            ->open([new Partition('dept', 'a')])
            ->append('fresh')
            ->close();
        $commit->commit([0]);

        static::assertSame(
            ['dept=a/orders.csv', 'dept=a/orders_backup.csv', 'dept=a/other.csv', 'dept=b/orders.csv'],
            WrittenFiles::under($this->out),
        );
        static::assertSame('fresh', $filesystem->readFrom($this->out->suffix('/dept=a/orders.csv'))->content());
    }

    public function test_overwrite_without_rows_empties_an_unpartitioned_part_destination(): void
    {
        $filesystem = native_local_filesystem();

        foreach (['part-00000.csv', 'part-00001.csv', 'other.csv'] as $file) {
            $filesystem
                ->writeTo($this->out->suffix('/' . $file))
                ->append('stale')
                ->close();
        }

        $commit = new FileCommit($filesystem, $this->out->suffix('/part-{part}.csv'), overwrite(), 'r1');
        $commit->admit(false);
        $commit->commit([0]);

        static::assertSame(['other.csv'], WrittenFiles::under($this->out));
    }

    public function test_overwrite_without_rows_removes_a_plain_file(): void
    {
        $filesystem = native_local_filesystem();
        $filesystem->writeTo($this->out->suffix('/orders.csv'))->append('stale')->close();

        $commit = new FileCommit($filesystem, $this->out->suffix('/orders.csv'), overwrite(), 'r1');
        $commit->admit(false);
        $commit->commit([0]);

        static::assertSame([], WrittenFiles::under($this->out));
    }

    public function test_overwrite_without_rows_keeps_every_partition(): void
    {
        $filesystem = native_local_filesystem();

        foreach (['a', 'b'] as $dept) {
            $filesystem->writeTo($this->out->suffix("/dept={$dept}/part-00000.csv"))->append('stale')->close();
        }

        $commit = new FileCommit($filesystem, $this->out->suffix('/part-{part}.csv'), overwrite(), 'r1');
        $commit->admit(true);
        $commit->commit([0]);

        static::assertSame(['dept=a/part-00000.csv', 'dept=b/part-00000.csv'], WrittenFiles::under($this->out));
    }

    public function test_a_move_failing_mid_commit_keeps_every_old_part(): void
    {
        $filesystem = native_local_filesystem();
        $destination = $this->out->suffix('/part-{part}.csv');

        foreach (range(0, 2) as $index) {
            $filesystem
                ->writeTo($this->out->suffix(sprintf('/part-%05d.csv', $index + 5)))
                ->append('old')
                ->close();
        }

        $failing = new FailingMvFilesystem($filesystem, healthyMoves: 1);
        $job = new FileCommit($failing, $destination, overwrite(), 'r1', 2);

        foreach (range(0, 1) as $index) {
            (new FileCommit($failing, $destination, overwrite(), 'r1', 2, $job->task($index)))
                ->open()
                ->append('new')
                ->close();
        }

        try {
            $job->commit([0, 1]);
            static::fail('commit() was expected to throw');
        } catch (FilesystemRuntimeException) {
        }

        $job->abort();

        static::assertSame(
            ['part-00000.csv', 'part-00005.csv', 'part-00006.csv', 'part-00007.csv'],
            WrittenFiles::under($this->out),
        );
    }

    #[DataProvider('modes_that_keep_parts')]
    public function test_a_mode_other_than_overwrite_removes_no_part(SaveMode $mode): void
    {
        $filesystem = memory_filesystem();
        $filesystem->writeTo(path('memory://out/part-00003.csv'))->append('stale')->close();

        (new FileCommit($filesystem, path('memory://out/part-{part}.csv'), $mode, 'r1'))->commit([0]);

        static::assertSame(['memory://out/part-00003.csv'], self::written($filesystem, 'memory://out/**/*'));
    }

    public function test_a_partitioned_write_over_two_tasks_commits_exactly_their_files(): void
    {
        $destination = $this->out->suffix('/part-{part}.csv');
        $job = new FileCommit(native_local_filesystem(), $destination, append(), 'r1', 2);
        $first = new FileCommit(native_local_filesystem(), $destination, append(), 'r1', 2, $job->task(0));
        $second = new FileCommit(native_local_filesystem(), $destination, append(), 'r1', 2, $job->task(1));
        $first
            ->open([new Partition('name', 'a')])
            ->append('1')
            ->close();
        $first
            ->open([new Partition('name', 'b')])
            ->append('2')
            ->close();
        $second
            ->open([new Partition('name', 'b')])
            ->append('3')
            ->close();

        $job->commit([1, 0]);

        static::assertSame(
            ['name=a/part-00000-r1.csv', 'name=b/part-00000-r1.csv', 'name=b/part-00001-r1.csv'],
            WrittenFiles::under($this->out),
        );
        static::assertSame([], WrittenFiles::temporaryOutputs($this->out));
    }

    public function test_only_the_tasks_whose_results_arrived_commit(): void
    {
        $destination = $this->out->suffix('/part-{part}.csv');
        $job = new FileCommit(native_local_filesystem(), $destination, exception_if_exists(), 'r1', 3);

        foreach (range(0, 2) as $index) {
            (new FileCommit(
                native_local_filesystem(),
                $destination,
                exception_if_exists(),
                'r1',
                3,
                $job->task($index),
            ))
                ->open()
                ->append('x')
                ->close();
        }

        $job->commit([0, 2]);

        static::assertSame(['part-00000.csv', 'part-00002.csv'], WrittenFiles::under($this->out));
        static::assertSame([], WrittenFiles::temporaryOutputs($this->out));
    }

    public function test_two_destinations_under_one_root_commit_only_their_own_files(): void
    {
        $commits = [];

        foreach (['/part-{part}.csv', '/x-{part}.json'] as $file) {
            $commits[$file] = new FileCommit(
                native_local_filesystem(),
                $this->out->suffix($file),
                overwrite(),
                'r1',
                2,
            );

            foreach (range(0, 1) as $index) {
                (new FileCommit(
                    native_local_filesystem(),
                    $this->out->suffix($file),
                    overwrite(),
                    'r1',
                    2,
                    $commits[$file]->task($index),
                ))
                    ->open()
                    ->append('x')
                    ->close();
            }
        }

        $commits['/part-{part}.csv']->commit([0, 1]);

        static::assertSame(
            [$this->out->path() . '/part-00000.csv', $this->out->path() . '/part-00001.csv'],
            glob($this->out->path() . '/*.*') ?: [],
        );
        static::assertCount(1, WrittenFiles::temporaryOutputs($this->out));

        $commits['/x-{part}.json']->abort();

        static::assertSame(['part-00000.csv', 'part-00001.csv'], WrittenFiles::under($this->out));
    }

    public function test_abort_removes_the_temporary_output_and_is_repeatable(): void
    {
        $destination = $this->out->suffix('/part-{part}.csv');
        $job = new FileCommit(native_local_filesystem(), $destination, overwrite(), 'r1', 2);
        (new FileCommit(native_local_filesystem(), $destination, overwrite(), 'r1', 2, $job->task(0)))
            ->open()
            ->append('x')
            ->close();

        $job->abort();
        $job->abort();

        static::assertSame([], WrittenFiles::under($this->out));
        static::assertSame([], WrittenFiles::temporaryOutputs($this->out));
    }

    public function test_a_task_abort_removes_only_its_own_files(): void
    {
        $destination = $this->out->suffix('/part-{part}.csv');
        $job = new FileCommit(native_local_filesystem(), $destination, overwrite(), 'r1', 2);
        $tasks = [];

        foreach (range(0, 1) as $index) {
            $tasks[$index] = new FileCommit(
                native_local_filesystem(),
                $destination,
                overwrite(),
                'r1',
                2,
                $job->task($index),
            );
            $tasks[$index]->open()->append('x');
        }

        $tasks[1]->abort();
        $tasks[0]->abort();
        $tasks[0] = new FileCommit(native_local_filesystem(), $destination, overwrite(), 'r1', 2, $job->task(0));
        $tasks[0]->open()->append('x')->close();
        $job->commit([0, 1]);

        static::assertSame(['part-00000.csv'], WrittenFiles::under($this->out));
    }

    #[DataProvider('modes')]
    public function test_abort_leaves_the_destination_untouched(SaveMode $mode): void
    {
        $filesystem = native_local_filesystem();
        $filesystem->writeTo($this->out->suffix('/kept.csv'))->append('old')->close();
        $commit = new FileCommit($filesystem, $this->out->suffix('/part-{part}.csv'), $mode, 'r1');
        $commit->admit(false);
        $commit->open()->append('half written');

        $commit->abort();

        static::assertSame(['kept.csv'], WrittenFiles::under($this->out));
    }
}
