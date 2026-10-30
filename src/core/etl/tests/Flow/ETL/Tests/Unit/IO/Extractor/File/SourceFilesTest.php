<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\IO\Extractor\File;

use Flow\ETL\IO\Extractor\Criteria;
use Flow\ETL\IO\Extractor\File\FileSplit;
use Flow\ETL\IO\Extractor\File\SourceFile;
use Flow\ETL\IO\Extractor\File\SourceFiles;
use Flow\ETL\Tests\Context\MemoryFiles;
use Flow\ETL\Tests\Double\CountingFilesystem;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Cardinality;
use Flow\Filesystem\Tests\Double\RejectingFilter;

use function array_map;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\native_local_filesystem;
use function Flow\Filesystem\DSL\path;

final class SourceFilesTest extends FlowTestCase
{
    public function test_a_sized_split_names_its_file_without_listing(): void
    {
        $counting = new CountingFilesystem(MemoryFiles::with(['memory://dir/a.csv' => 'id\n1\n']));

        static::assertEquals(
            [new SourceFile(path('memory://dir/a.csv'), 7)],
            (new SourceFiles($counting, path('memory://dir/*.csv')))->of((new Criteria())->withSplit(
                new FileSplit(path('memory://dir/a.csv'), 0, null, 7),
            )),
        );
        static::assertSame(0, $counting->listCalls);
    }

    public function test_an_unsized_split_lists_its_path(): void
    {
        $counting = new CountingFilesystem(MemoryFiles::with([
            'memory://dir/a.csv' => 'id',
            'memory://dir/b.csv' => 'id',
        ]));

        static::assertEquals(
            [new SourceFile(path('memory://dir/b.csv'), 2)],
            (new SourceFiles($counting, path('memory://dir/*.csv')))->of((new Criteria())->withSplit(
                new FileSplit(path('memory://dir/b.csv'), 0, null),
            )),
        );
        static::assertSame(1, $counting->listCalls);
    }

    public function test_no_split_reads_what_the_path_filter_admits(): void
    {
        $files = new SourceFiles(MemoryFiles::with([
            'memory://dir/a.csv' => 'id',
            'memory://dir/b.csv' => 'id',
        ]), path('memory://dir/*.csv'));

        static::assertSame(
            ['memory://dir/a.csv', 'memory://dir/b.csv'],
            array_map(static fn(SourceFile $source): string => $source->uri(), $files->of(new Criteria())),
        );
        static::assertSame([], $files->of(new Criteria(pathFilter: new RejectingFilter())));
        static::assertSame([], $files->statuses(new Criteria(pathFilter: new RejectingFilter())));
    }

    public function test_every_answer_reads_one_listing(): void
    {
        $counting = new CountingFilesystem(MemoryFiles::with([
            'memory://orders/year=2024/a.csv' => 'a',
            'memory://orders/year=2025/b.csv' => 'bb',
        ]));
        $files = new SourceFiles($counting, path('memory://orders/*/*.csv'));

        $files->of(new Criteria());
        $files->of(new Criteria(pathFilter: new RejectingFilter()));
        $files->statuses();
        $files->partitionNames();
        $files->count();
        $files->bytes();
        $files->perFile();
        $files->byteWindows(1);

        static::assertSame(1, $counting->listCalls);
    }

    public function test_refresh_lists_the_source_again(): void
    {
        $filesystem = MemoryFiles::with(['memory://orders/year=2024/a.csv' => 'a']);
        $files = new SourceFiles($filesystem, path('memory://orders/*/*.csv'));
        static::assertSame(['year' => false], $files->partitionNames());

        $filesystem->writeTo(path('memory://orders/plain/b.csv'))->close();

        static::assertSame(1, $files->count());

        $files->refresh();

        static::assertSame(2, $files->count());
        static::assertSame(['year' => true], $files->partitionNames());
    }

    public function test_windows_of_consecutive_files_follow_the_listing_order(): void
    {
        static::assertEquals(
            [
                new FileSplit(path('memory://orders/a.csv'), 0, 1, 2),
                new FileSplit(path('memory://orders/a.csv'), 1, 1, 2),
                new FileSplit(path('memory://orders/c.csv'), 0, 1, 1),
            ],
            (new SourceFiles(MemoryFiles::with([
                'memory://orders/a.csv' => 'ab',
                'memory://orders/c.csv' => 'c',
            ]), path('memory://orders/*.csv')))->byteWindows(1),
        );
    }

    public function test_an_empty_listing_is_one_split_over_the_path(): void
    {
        $files = new SourceFiles(memory_filesystem(), path('memory://orders/*.csv'));

        static::assertEquals([new FileSplit(path('memory://orders/*.csv'), 0, null)], $files->byteWindows(1));
        static::assertEquals([new FileSplit(path('memory://orders/*.csv'), 0, null)], $files->perFile());
    }

    public function test_one_split_per_listed_file_the_path_filter_admits(): void
    {
        $files = new SourceFiles(MemoryFiles::with([
            'memory://orders/a.csv' => 'a',
            'memory://orders/b.csv' => 'bb',
        ]), path('memory://orders/*.csv'));

        static::assertEquals(
            [
                new FileSplit(path('memory://orders/a.csv'), 0, null, 1),
                new FileSplit(path('memory://orders/b.csv'), 0, null, 2),
            ],
            $files->perFile(),
        );
        static::assertEquals(
            [new FileSplit(path('memory://orders/*.csv'), 0, null)],
            $files->perFile(new RejectingFilter()),
        );
    }

    public function test_it_counts_and_sums_the_listed_files(): void
    {
        $files = new SourceFiles(MemoryFiles::with([
            'memory://orders/a.csv' => 'a',
            'memory://orders/b.csv' => 'bb',
        ]), path('memory://orders/*.csv'));

        static::assertSame(2, $files->count());
        static::assertEquals(Cardinality::exact(3), $files->bytes());
    }

    public function test_an_empty_listing_is_zero_files_and_zero_bytes_exactly(): void
    {
        $files = new SourceFiles(memory_filesystem(), path('memory://orders/*.csv'));

        static::assertSame(0, $files->count());
        static::assertEquals(Cardinality::exact(0), $files->bytes());
    }

    public function test_a_zero_byte_file_reported_without_a_size_makes_the_total_unknown(): void
    {
        $files = new SourceFiles(native_local_filesystem(), path(__DIR__ . '/../../../../Fixtures/empty.txt'));

        static::assertSame(1, $files->count());
        static::assertEquals(Cardinality::unknown(), $files->bytes());
    }

    public function test_a_listing_without_partitions_names_none(): void
    {
        static::assertSame(
            [],
            (new SourceFiles(MemoryFiles::with([
                'memory://plain/data.csv' => 'id',
            ]), path('memory://plain/*.csv')))->partitionNames(),
        );
    }

    public function test_partition_names_are_sorted_by_name_not_by_path_order(): void
    {
        static::assertSame(
            ['month' => false, 'region' => false, 'year' => false],
            (new SourceFiles(MemoryFiles::with([
                'memory://sorted/year=2026/month=01/region=eu/data.csv' => 'id',
            ]), path('memory://sorted/*/*/*/*.csv')))->partitionNames(),
        );
    }

    public function test_a_partition_only_some_paths_carry_is_nullable(): void
    {
        static::assertSame(
            ['date' => true],
            (new SourceFiles(MemoryFiles::with([
                'memory://mixed/date=2026-01-01/data.csv' => 'id',
                'memory://mixed/plain/data.csv' => 'id',
            ]), path('memory://mixed/*/*.csv')))->partitionNames(),
        );
    }
}
