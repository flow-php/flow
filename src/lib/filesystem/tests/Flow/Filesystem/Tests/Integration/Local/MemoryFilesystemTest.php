<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Integration\Local;

use Flow\Filesystem\Exception\InvalidArgumentException;
use Flow\Filesystem\Exception\InvalidSchemeException;
use Flow\Filesystem\FileStatus;
use Flow\Filesystem\Local\MemoryFilesystem;
use Flow\Filesystem\Path\Filter\KeepAll;
use Flow\Filesystem\Tests\Integration\NativeLocalFilesystemTestCase;

use function array_map;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;
use function fopen;
use function iterator_to_array;
use function sort;

final class MemoryFilesystemTest extends NativeLocalFilesystemTestCase
{
    public function test_append_to_rejects_mismatched_scheme(): void
    {
        $this->expectException(InvalidSchemeException::class);
        $this->expectExceptionMessage(
            'Scheme "file://" is not supported by this protocol. Expected scheme is "memory://"',
        );

        memory_filesystem()->appendTo(path('file:///var/foo.txt'));
    }

    public function test_appending_after_ranged_read_appends_at_the_end(): void
    {
        $fs = memory_filesystem();

        $stream = $fs->writeTo($path = path('memory://file'));
        $stream->append('AAAABBBB');
        $stream->close();

        static::assertSame('BBBB', $fs->readFrom($path)->read(4, 4));

        $stream = $fs->appendTo($path);
        $stream->append('CCCC');
        $stream->close();

        static::assertSame('AAAABBBBCCCC', $fs->readFrom($path)->content());
    }

    public function test_appending_to_existing_blob(): void
    {
        $fs = memory_filesystem();

        $stream = $fs->writeTo($path = path('memory://file'));
        $stream->append("This is first line\n");
        $stream->close();

        $stream = $fs->appendTo($path);
        $stream->append("This is second line\n");
        $stream->close();

        $status = $fs->status($path);
        static::assertNotNull($status);
        static::assertTrue($status->isFile());
        static::assertFalse($status->isDirectory());
        static::assertSame(<<<'TXT'
            This is first line
            This is second line

            TXT, $fs->readFrom($path)->content());

        $fs->rm($path);
    }

    public function test_dir_exists(): void
    {
        $fs = memory_filesystem();
        $fs->writeTo($path = path('memory://file'));

        $status = $fs->status($path);
        static::assertNotNull($status);
        static::assertTrue($status->isFile());
        static::assertFalse($status->isDirectory());
        static::assertNull($fs->status(path('memory:///not_existing_directory')));
    }

    public function test_file_status_on_existing_file(): void
    {
        $fs = memory_filesystem();

        $resource = fopen(__DIR__ . '/../Fixtures/orders.csv', 'rb');
        static::assertIsResource($resource);
        $fs->writeTo(path('memory:///var/file.txt'))->fromResource($resource);

        $status = $fs->status(path('memory:///var/file.txt'));
        static::assertNotNull($status);
        static::assertTrue($status->isFile());
        $fs->rm(path('memory:///var/file.txt'));
    }

    public function test_file_status_on_non_existing_file(): void
    {
        $fs = memory_filesystem();

        static::assertNull($fs->status(path('memory:///var/non-existing-file.txt')));
    }

    public function test_file_status_on_non_existing_pattern(): void
    {
        $fs = memory_filesystem();

        static::assertNull($fs->status(path('memory:///var/non-existing-folder/*')));
    }

    public function test_file_status_on_partial_path(): void
    {
        $fs = memory_filesystem();

        $resource = fopen(__DIR__ . '/../Fixtures/orders.csv', 'rb');
        static::assertIsResource($resource);
        $fs->writeTo(path('memory:///var/some_path_to/file.txt'))->fromResource($resource);

        static::assertNull($fs->status(path('memory:///var/some_path')));
    }

    public function test_file_status_on_pattern(): void
    {
        $fs = memory_filesystem();

        $resource = fopen(__DIR__ . '/../Fixtures/orders.csv', 'rb');
        static::assertIsResource($resource);
        $fs->writeTo(path('memory:///var/some_path_to/file.txt'))->fromResource($resource);

        $status = $fs->status(path('memory:///var/some_path_to/file.txt'));
        static::assertNotNull($status);
        static::assertTrue($status->isFile());

        $patternStatus = $fs->status(path('memory:///var/some_path_to/*.txt'));
        static::assertNotNull($patternStatus);
        static::assertSame('memory://var/some_path_to/file.txt', $patternStatus->path->uri());
    }

    public function test_list_files_matching_partition_placeholder_pattern(): void
    {
        $fs = memory_filesystem();

        $fs->writeTo(path('memory:///var/placeholders/order-year=2024/123456-PL.csv'))->append('data');
        $fs->writeTo(path('memory:///var/placeholders/order-year=2024/789-DE.csv'))->append('data');
        $fs->writeTo(path('memory:///var/placeholders/order-year=2024/ignored.txt'))->append('data');

        $files = iterator_to_array($fs->list(path('memory:///var/placeholders/order-year=2024/{order-name}.csv')));

        $basenames = array_map(static fn(FileStatus $status) => $status->path->basename(), $files);
        sort($basenames);

        static::assertSame(['123456-PL.csv', '789-DE.csv'], $basenames);
    }

    public function test_list_rejects_mismatched_scheme(): void
    {
        $this->expectException(InvalidSchemeException::class);

        iterator_to_array(memory_filesystem()->list(path('file:///var/foo.txt')));
    }

    public function test_memory_filesystem_does_not_support_a_local_path(): void
    {
        static::assertFalse(memory_filesystem()->supports(path('/tmp/orders.csv')));
        static::assertTrue(memory_filesystem()->supports(path('memory://orders.csv')));
    }

    public function test_move_a_file(): void
    {
        $fs = memory_filesystem();
        $fs->writeTo(path('memory:///var/file.txt'))->append('Hello, World!');

        static::assertTrue($fs->mv(path('memory:///var/file.txt'), path('memory:///var/file_mv.txt')));
        static::assertNull($fs->status(path('memory:///var/file.txt')));
        static::assertSame('Hello, World!', $fs->readFrom(path('memory:///var/file_mv.txt'))->content());
    }

    public function test_move_a_directory_rebases_every_file_under_it(): void
    {
        $fs = memory_filesystem();
        $fs->writeTo(path('memory://tmp/a.txt'))->append('a');
        $fs->writeTo(path('memory://tmp/nested/b.txt'))->append('b');
        $fs->writeTo(path('memory://tmp-sibling.txt'))->append('c');

        static::assertTrue($fs->mv(path('memory://tmp'), path('memory://out')));
        static::assertSame('a', $fs->readFrom(path('memory://out/a.txt'))->content());
        static::assertSame('b', $fs->readFrom(path('memory://out/nested/b.txt'))->content());
        static::assertNull($fs->status(path('memory://tmp/a.txt')));
        static::assertNull($fs->status(path('memory://tmp/nested/b.txt')));
        static::assertSame('c', $fs->readFrom(path('memory://tmp-sibling.txt'))->content());
    }

    public function test_move_replaces_an_existing_file(): void
    {
        $fs = memory_filesystem();
        $fs->writeTo(path('memory://new.txt'))->append('new');
        $fs->writeTo(path('memory://old.txt'))->append('old content');

        static::assertTrue($fs->mv(path('memory://new.txt'), path('memory://old.txt')));
        static::assertSame('new', $fs->readFrom(path('memory://old.txt'))->content());
        static::assertNull($fs->status(path('memory://new.txt')));
    }

    public function test_move_a_missing_source_returns_false(): void
    {
        $fs = memory_filesystem();

        static::assertFalse($fs->mv(path('memory://missing.txt'), path('memory://target.txt')));
        static::assertNull($fs->status(path('memory://target.txt')));
    }

    public function test_move_rejects_a_mismatched_scheme(): void
    {
        $this->expectException(InvalidSchemeException::class);

        memory_filesystem()->mv(path('memory://a.txt'), path('file:///tmp/a.txt'));
    }

    public function test_read_from_rejects_mismatched_scheme(): void
    {
        $this->expectException(InvalidSchemeException::class);

        memory_filesystem()->readFrom(path('file:///var/foo.txt'));
    }

    public function test_remove_file_when_exists(): void
    {
        $fs = memory_filesystem();

        $stream = $fs->writeTo(path('memory:///var/flow-fs-test/remove_file_when_exists.txt'));
        $stream->append('some data to make file not empty');

        $status = $fs->status($stream->path());
        static::assertNotNull($status);
        static::assertTrue($status->isFile());

        static::assertTrue($fs->rm($stream->path()));

        static::assertNull($fs->status($stream->path()));
    }

    public function test_remove_directory_removes_every_file_under_it(): void
    {
        $fs = memory_filesystem();
        $fs->writeTo(path('memory://dir/a.txt'))->append('a');
        $fs->writeTo(path('memory://dir/nested/b.txt'))->append('b');
        $fs->writeTo(path('memory://dir-sibling.txt'))->append('c');

        static::assertTrue($fs->rm(path('memory://dir')));
        static::assertNull($fs->status(path('memory://dir/a.txt')));
        static::assertNull($fs->status(path('memory://dir/nested/b.txt')));
        static::assertNotNull($fs->status(path('memory://dir-sibling.txt')));
    }

    public function test_remove_missing_directory_returns_false(): void
    {
        static::assertFalse(memory_filesystem()->rm(path('memory://missing')));
    }

    public function test_remove_pattern(): void
    {
        $fs = memory_filesystem();
        $stream = $fs->writeTo(path('memory:///remove_file_when_exists.txt'))->append(
            'some data to make file not empty',
        );

        $fs->rm(path('memory:///*.txt'));
        static::assertNull($fs->status($stream->path()));
        static::assertEmpty(iterator_to_array($fs->list(path('memory:///*.txt'), new KeepAll())));
    }

    public function test_rm_rejects_mismatched_scheme(): void
    {
        $this->expectException(InvalidSchemeException::class);

        memory_filesystem()->rm(path('file:///var/foo.txt'));
    }

    public function test_status_rejects_mismatched_scheme(): void
    {
        $this->expectException(InvalidSchemeException::class);

        memory_filesystem()->status(path('file:///var/foo.txt'));
    }

    public function test_that_scan_sort_files_by_path_names(): void
    {
        $fs = memory_filesystem();

        $fs->writeTo(path('memory:///var/multi_partitions/date=2022-01-03/country=de/file.txt'))->append('hello world');
        $fs->writeTo(path('memory:///var/multi_partitions/date=2022-01-02/country=pl/file.txt'))->append('hello world');
        $fs->writeTo(path('memory:///var/multi_partitions/date=2022-01-01/country=pl/file.txt'))->append('hello world');
        $fs->writeTo(path('memory:///var/multi_partitions/date=2022-01-01/country=de/file.txt'))->append('hello world');
        $fs->writeTo(path('memory:///var/multi_partitions/date=2022-01-03/country=pl/file.txt'))->append('hello world');
        $fs->writeTo(path('memory:///var/multi_partitions/date=2022-01-02/country=de/file.txt'))->append('hello world');
        $fs->writeTo(path('memory:///var/multi_partitions/date=2022-01-04/country=pl/file.txt'))->append('hello world');
        $fs->writeTo(path('memory:///var/multi_partitions/date=2022-01-05/country=de/file.txt'))->append('hello world');
        $fs->writeTo(path('memory:///var/multi_partitions/date=2022-01-04/country=de/file.txt'))->append('hello world');
        $fs->writeTo(path('memory:///var/multi_partitions/date=2022-01-05/country=pl/file.txt'))->append('hello world');

        $statuses = iterator_to_array($fs->list(path('memory:///var/multi_partitions/**/*.txt')));

        $uris = array_map(static fn(FileStatus $s): string => $s->path->uri(), $statuses);

        static::assertSame(
            [
                path('memory:///var/multi_partitions/date=2022-01-01/country=de/file.txt')->uri(),
                path('memory:///var/multi_partitions/date=2022-01-01/country=pl/file.txt')->uri(),
                path('memory:///var/multi_partitions/date=2022-01-02/country=de/file.txt')->uri(),
                path('memory:///var/multi_partitions/date=2022-01-02/country=pl/file.txt')->uri(),
                path('memory:///var/multi_partitions/date=2022-01-03/country=de/file.txt')->uri(),
                path('memory:///var/multi_partitions/date=2022-01-03/country=pl/file.txt')->uri(),
                path('memory:///var/multi_partitions/date=2022-01-04/country=de/file.txt')->uri(),
                path('memory:///var/multi_partitions/date=2022-01-04/country=pl/file.txt')->uri(),
                path('memory:///var/multi_partitions/date=2022-01-05/country=de/file.txt')->uri(),
                path('memory:///var/multi_partitions/date=2022-01-05/country=pl/file.txt')->uri(),
            ],
            $uris,
        );
    }

    public function test_tmp_dir(): void
    {
        $fs = memory_filesystem();

        $this->expectExceptionMessage('Memory does not have a system tmp directory');

        $fs->getSystemTmpDir();
    }

    public function test_write_to_rejects_mismatched_scheme(): void
    {
        $this->expectException(InvalidSchemeException::class);

        memory_filesystem()->writeTo(path('file:///var/foo.txt'));
    }

    public function test_a_memory_filesystem_has_no_configuration(): void
    {
        static::assertNull((new MemoryFilesystem())->normalize());
    }

    public function test_a_memory_filesystem_cannot_be_rebuilt_from_a_configuration(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'MemoryFilesystem holds in-process state and cannot be rebuilt from a configuration',
        );

        MemoryFilesystem::fromArray([]);
    }
}
