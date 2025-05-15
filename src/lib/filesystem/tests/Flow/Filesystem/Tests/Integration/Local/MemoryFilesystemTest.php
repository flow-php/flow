<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Integration\Local;

use function Flow\ETL\DSL\{all, lit, ref};
use function Flow\Filesystem\DSL\{memory_filesystem, path_memory};
use Flow\ETL\Filesystem\{ScalarFunctionFilter};
use Flow\ETL\Row\EntryFactory;
use Flow\Filesystem\{FileStatus,
    Tests\Integration\NativeLocalFilesystemTestCase};
use Flow\Filesystem\Path\Filter\KeepAll;
use Flow\Types\Type\{AutoCaster};

final class MemoryFilesystemTest extends NativeLocalFilesystemTestCase
{
    public function test_appending_to_existing_blob() : void
    {
        $fs = memory_filesystem();

        $stream = $fs->writeTo($path = path_memory('file'));
        $stream->append("This is first line\n");
        $stream->close();

        $stream = $fs->appendTo($path);
        $stream->append("This is second line\n");
        $stream->close();

        self::assertTrue($fs->status($path)->isFile());
        self::assertFalse($fs->status($path)->isDirectory());
        self::assertSame(
            <<<'TXT'
This is first line
This is second line

TXT
            ,
            $fs->readFrom($path)->content()
        );

        $fs->rm($path);
    }

    public function test_dir_exists() : void
    {
        $fs = memory_filesystem();
        $fs->writeTo($path = path_memory('file'));

        self::assertTrue($fs->status($path)->isFile());
        self::assertFalse($fs->status($path)->isDirectory()); // there are no folders in memory
        self::assertNull($fs->status(path_memory('/not_existing_directory')));
    }

    public function test_file_status_on_existing_file() : void
    {
        $fs = memory_filesystem();

        $fs->writeTo(path_memory('/var/file.txt'))->fromResource(\fopen(__DIR__ . '/../Fixtures/orders.csv', 'rb'));

        self::assertTrue($fs->status(path_memory('/var/file.txt'))->isFile());
        $fs->rm(path_memory('/var/file.txt'));
    }

    public function test_file_status_on_non_existing_file() : void
    {
        $fs = memory_filesystem();

        self::assertNull($fs->status(path_memory('/var/non-existing-file.txt')));
    }

    public function test_file_status_on_non_existing_pattern() : void
    {
        $fs = memory_filesystem();

        self::assertNull($fs->status(path_memory('/var/non-existing-folder/*')));
    }

    public function test_file_status_on_partial_path() : void
    {
        $fs = memory_filesystem();

        $fs->writeTo(path_memory('/var/some_path_to/file.txt'))->fromResource(\fopen(__DIR__ . '/../Fixtures/orders.csv', 'rb'));

        self::assertNull($fs->status(path_memory('/var/some_path')));
    }

    public function test_file_status_on_pattern() : void
    {
        $fs = memory_filesystem();

        $fs->writeTo(path_memory('/var/some_path_to/file.txt'))->fromResource(\fopen(__DIR__ . '/../Fixtures/orders.csv', 'rb'));

        self::assertTrue($fs->status(path_memory('/var/some_path_to/file.txt'))->isFile());
        self::assertSame(
            'memory://var/some_path_to/file.txt',
            $fs->status(path_memory('/var/some_path_to/*.txt'))->path->uri()
        );
    }

    public function test_move_blob() : void
    {
        $fs = memory_filesystem();

        $fs->writeTo(path_memory('/var/file.txt'))->append('Hello, World!');

        $this->expectExceptionMessage('Cannot move files around in memory');

        $fs->mv(path_memory('/var/file.txt'), path_memory('/var/file_mv.txt'));
    }

    public function test_reading_multi_partitioned_path() : void
    {
        $fs = memory_filesystem();

        $fs->writeTo(path_memory('/var/multi_partitions/date=2022-01-01/country=de/file.txt'))->append('Hello, World!');
        $fs->writeTo(path_memory('/var/multi_partitions/date=2022-01-01/country=pl/file.txt'))->append('Hello, World!');
        $fs->writeTo(path_memory('/var/multi_partitions/date=2022-01-02/country=de/file.txt'))->append('Hello, World!');
        $fs->writeTo(path_memory('/var/multi_partitions/date=2022-01-02/country=pl/file.txt'))->append('Hello, World!');
        $fs->writeTo(path_memory('/var/multi_partitions/date=2022-01-03/country=de/file.txt'))->append('Hello, World!');
        $fs->writeTo(path_memory('/var/multi_partitions/date=2022-01-03/country=pl/file.txt'))->append('Hello, World!');

        $paths = \iterator_to_array(
            $fs
                ->list(
                    path_memory('/var/multi_partitions/**/*.txt'),
                    new ScalarFunctionFilter(
                        all(
                            ref('country')->equals(lit('pl')),
                            all(
                                ref('date')->cast('date')->greaterThanEqual(lit(new \DateTimeImmutable('2022-01-02'))),
                                ref('date')->cast('date')->lessThan(lit(new \DateTimeImmutable('2022-01-04')))
                            )
                        ),
                        new EntryFactory(),
                        new AutoCaster()
                    )
                )
        );
        \sort($paths);

        $path1 = path_memory('/var/multi_partitions/date=2022-01-02/country=pl/file.txt');
        $path1->partitions();
        $path2 = path_memory('/var/multi_partitions/date=2022-01-03/country=pl/file.txt');
        $path2->partitions();

        self::assertEquals(
            [
                new FileStatus($path1, true),
                new FileStatus($path2, true),
            ],
            $paths
        );
    }

    public function test_remove_file_when_exists() : void
    {
        $fs = memory_filesystem();

        $stream = $fs->writeTo(path_memory('/var/flow-fs-test/remove_file_when_exists.txt'));
        $stream->append('some data to make file not empty');

        self::assertTrue($fs->status($stream->path())->isFile());

        self::assertTrue($fs->rm($stream->path()));

        self::assertNull($fs->status($stream->path()));
    }

    public function test_remove_pattern() : void
    {
        $fs = memory_filesystem();
        $stream = $fs->writeTo(path_memory('/remove_file_when_exists.txt'))
            ->append('some data to make file not empty');

        $fs->rm(path_memory('/*.txt'));
        self::assertNull($fs->status($stream->path()));
        self::assertEmpty(\iterator_to_array($fs->list(path_memory('/*.txt'), new KeepAll())));
    }

    public function test_that_scan_sort_files_by_path_names() : void
    {
        $fs = memory_filesystem();

        $fs->writeTo(path_memory('/var/multi_partitions/date=2022-01-03/country=de/file.txt'))->append('hello world');
        $fs->writeTo(path_memory('/var/multi_partitions/date=2022-01-02/country=pl/file.txt'))->append('hello world');
        $fs->writeTo(path_memory('/var/multi_partitions/date=2022-01-01/country=pl/file.txt'))->append('hello world');
        $fs->writeTo(path_memory('/var/multi_partitions/date=2022-01-01/country=de/file.txt'))->append('hello world');
        $fs->writeTo(path_memory('/var/multi_partitions/date=2022-01-03/country=pl/file.txt'))->append('hello world');
        $fs->writeTo(path_memory('/var/multi_partitions/date=2022-01-02/country=de/file.txt'))->append('hello world');
        $fs->writeTo(path_memory('/var/multi_partitions/date=2022-01-04/country=pl/file.txt'))->append('hello world');
        $fs->writeTo(path_memory('/var/multi_partitions/date=2022-01-05/country=de/file.txt'))->append('hello world');
        $fs->writeTo(path_memory('/var/multi_partitions/date=2022-01-04/country=de/file.txt'))->append('hello world');
        $fs->writeTo(path_memory('/var/multi_partitions/date=2022-01-05/country=pl/file.txt'))->append('hello world');

        $paths = \iterator_to_array(
            $fs->list(path_memory('/var/multi_partitions/**/*.txt'))
        );

        self::assertEquals(
            [
                new FileStatus(path_memory('/var/multi_partitions/date=2022-01-01/country=de/file.txt'), true),
                new FileStatus(path_memory('/var/multi_partitions/date=2022-01-01/country=pl/file.txt'), true),
                new FileStatus(path_memory('/var/multi_partitions/date=2022-01-02/country=de/file.txt'), true),
                new FileStatus(path_memory('/var/multi_partitions/date=2022-01-02/country=pl/file.txt'), true),
                new FileStatus(path_memory('/var/multi_partitions/date=2022-01-03/country=de/file.txt'), true),
                new FileStatus(path_memory('/var/multi_partitions/date=2022-01-03/country=pl/file.txt'), true),
                new FileStatus(path_memory('/var/multi_partitions/date=2022-01-04/country=de/file.txt'), true),
                new FileStatus(path_memory('/var/multi_partitions/date=2022-01-04/country=pl/file.txt'), true),
                new FileStatus(path_memory('/var/multi_partitions/date=2022-01-05/country=de/file.txt'), true),
                new FileStatus(path_memory('/var/multi_partitions/date=2022-01-05/country=pl/file.txt'), true),
            ],
            $paths
        );
    }

    public function test_tmp_dir() : void
    {
        $fs = memory_filesystem();

        $this->expectExceptionMessage('Memory does not have a system tmp directory');

        $fs->getSystemTmpDir();
    }
}
