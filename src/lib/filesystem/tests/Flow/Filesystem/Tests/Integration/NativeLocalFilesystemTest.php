<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Integration;

use DateTimeImmutable;
use Flow\ETL\Filesystem\ScalarFunctionFilter;
use Flow\Filesystem\Exception\InvalidSchemeException;
use Flow\Filesystem\FileStatus;
use Flow\Filesystem\Path\Filter\KeepAll;
use Flow\Filesystem\Stream\NativeLocalDestinationStream;
use PHPUnit\Framework\Attributes\TestWith;

use function array_map;
use function file_exists;
use function file_get_contents;
use function Flow\ETL\DSL\all;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\Filesystem\DSL\native_local_filesystem;
use function Flow\Filesystem\DSL\path;
use function Flow\Filesystem\DSL\path_real;
use function fopen;
use function iterator_to_array;
use function mb_substr;
use function mkdir;

final class NativeLocalFilesystemTest extends NativeLocalFilesystemTestCase
{
    protected function setUp(): void
    {
        if (!file_exists(__DIR__ . '/var')) {
            mkdir(__DIR__ . '/var');
        }
    }

    public function test_append_to_rejects_mismatched_scheme(): void
    {
        $this->expectException(InvalidSchemeException::class);
        $this->expectExceptionMessage(
            'Scheme "memory://" is not supported by this protocol. Expected scheme is "file://"',
        );

        native_local_filesystem()->appendTo(path('memory:///var/foo.txt'));
    }

    public function test_appending_to_existing_blob(): void
    {
        $fs = native_local_filesystem();

        $stream = $fs->writeTo(path(__DIR__ . '/var/file.txt'));
        $stream->append("This is first line\n");
        $stream->close();

        $stream = $fs->appendTo(path(__DIR__ . '/var/file.txt'));
        $stream->append("This is second line\n");
        $stream->close();

        $status = $fs->status(path(__DIR__ . '/var/file.txt'));
        static::assertNotNull($status);
        static::assertTrue($status->isFile());
        static::assertFalse($status->isDirectory());
        static::assertSame(<<<'TXT'
            This is first line
            This is second line

            TXT, $fs->readFrom(path(__DIR__ . '/var/file.txt'))->content());

        $fs->rm(path(__DIR__ . '/var/file.txt'));
    }

    public function test_dir_exists(): void
    {
        $status = native_local_filesystem()->status(path(__DIR__));
        static::assertNotNull($status);
        static::assertFalse($status->isFile());
        static::assertTrue($status->isDirectory());
        static::assertNull(native_local_filesystem()->status(path(__DIR__ . '/not_existing_directory')));
    }

    public function test_fie_exists(): void
    {
        $status = native_local_filesystem()->status(path(__FILE__));
        static::assertNotNull($status);
        static::assertTrue($status->isFile());
        static::assertFalse($status->isDirectory());
        static::assertNull(native_local_filesystem()->status(path(__DIR__ . '/not_existing_file.php')));
    }

    public function test_file_pattern_exists(): void
    {
        $status = native_local_filesystem()->status(path(__DIR__ . '/**/*.txt'));
        static::assertNotNull($status);
        static::assertTrue($status->isFile());
        static::assertNull(native_local_filesystem()->status(path(__DIR__ . '/**/*.pdf')));
    }

    public function test_file_status_on_existing_file(): void
    {
        $fs = native_local_filesystem();

        $resource = fopen(__DIR__ . '/Fixtures/orders.csv', 'rb');
        static::assertIsResource($resource);
        $fs->writeTo(path(__DIR__ . '/var/file.txt'))->fromResource($resource);

        $status = $fs->status(path(__DIR__ . '/var/file.txt'));
        static::assertNotNull($status);
        static::assertTrue($status->isFile());
    }

    public function test_file_status_on_existing_folder(): void
    {
        $fs = native_local_filesystem();

        $resource = fopen(__DIR__ . '/Fixtures/orders.csv', 'rb');
        static::assertIsResource($resource);
        $fs->writeTo(path(__DIR__ . '/var/nested/orders/orders.txt'))->fromResource($resource);

        $status1 = $fs->status(path(__DIR__ . '/var/nested/orders'));
        static::assertNotNull($status1);
        static::assertTrue($status1->isDirectory());

        $status2 = $fs->status(path(__DIR__ . '/var/nested/orders/'));
        static::assertNotNull($status2);
        static::assertTrue($status2->isDirectory());
    }

    public function test_file_status_on_non_existing_file(): void
    {
        $fs = native_local_filesystem();

        static::assertNull($fs->status(path(__DIR__ . '/var/non-existing-file.txt')));
    }

    public function test_file_status_on_non_existing_folder(): void
    {
        $fs = native_local_filesystem();

        static::assertNull($fs->status(path(__DIR__ . '/var/non-existing-folder/')));
    }

    public function test_file_status_on_non_existing_pattern(): void
    {
        $fs = native_local_filesystem();

        static::assertNull($fs->status(path(__DIR__ . '/var/non-existing-folder/*')));
    }

    public function test_file_status_on_partial_path(): void
    {
        $fs = native_local_filesystem();

        $resource = fopen(__DIR__ . '/Fixtures/orders.csv', 'rb');
        static::assertIsResource($resource);
        $fs->writeTo(path(__DIR__ . '/var/some_path_to/file.txt'))->fromResource($resource);

        static::assertNull($fs->status(path(__DIR__ . '/var/some_path')));
    }

    public function test_file_status_on_root_folder(): void
    {
        $fs = native_local_filesystem();

        $status = $fs->status(path(__DIR__ . '/var/'));
        static::assertNotNull($status);
        static::assertTrue($status->isDirectory());
    }

    public function test_list_files_matching_partition_placeholder_pattern(): void
    {
        $fs = native_local_filesystem();

        $fs->writeTo(path(__DIR__ . '/var/placeholders/order-year=2024/123456-PL.csv'))->append('data');
        $fs->writeTo(path(__DIR__ . '/var/placeholders/order-year=2024/789-DE.csv'))->append('data');
        $fs->writeTo(path(__DIR__ . '/var/placeholders/order-year=2024/ignored.txt'))->append('data');

        $files = iterator_to_array($fs->list(path(__DIR__ . '/var/placeholders/order-year=2024/{order-name}.csv')));

        static::assertSame(
            ['123456-PL.csv', '789-DE.csv'],
            array_map(static fn(FileStatus $status) => $status->path->basename(), $files),
        );
    }

    public function test_list_matches_are_sorted_by_path(): void
    {
        $fs = native_local_filesystem();
        $fs->rm(path(__DIR__ . '/var/list_order'));

        foreach (['c', 'a', 'b'] as $directory) {
            foreach (['file_02.txt', 'file_01.txt'] as $file) {
                $fs->writeTo(path(__DIR__ . '/var/list_order/' . $directory . '/' . $file))->append('data');
            }
        }

        $expected = [
            path(__DIR__ . '/var/list_order/a/file_01.txt')->uri(),
            path(__DIR__ . '/var/list_order/a/file_02.txt')->uri(),
            path(__DIR__ . '/var/list_order/b/file_01.txt')->uri(),
            path(__DIR__ . '/var/list_order/b/file_02.txt')->uri(),
            path(__DIR__ . '/var/list_order/c/file_01.txt')->uri(),
            path(__DIR__ . '/var/list_order/c/file_02.txt')->uri(),
        ];

        static::assertSame($expected, array_map(
            static fn(FileStatus $status): string => $status->path->uri(),
            iterator_to_array($fs->list(path(__DIR__ . '/var/list_order/*/*.txt'), new KeepAll())),
        ));
        static::assertSame($expected, array_map(
            static fn(FileStatus $status): string => $status->path->uri(),
            iterator_to_array($fs->list(path(__DIR__ . '/var/list_order/**/*.txt'), new KeepAll())),
        ));
    }

    public function test_list_rejects_mismatched_scheme(): void
    {
        $this->expectException(InvalidSchemeException::class);

        iterator_to_array(native_local_filesystem()->list(path('memory:///var/foo.txt')));
    }

    public function test_move_blob(): void
    {
        $fs = native_local_filesystem();

        $fs->writeTo(path(__DIR__ . '/var/file.txt'))->append('Hello, World!');

        $fs->mv(path(__DIR__ . '/var/file.txt'), path(__DIR__ . '/var/file_mv.txt'));

        static::assertNull($fs->status(path(__DIR__ . '/var/file.txt')));
        static::assertSame('Hello, World!', $fs->readFrom(path(__DIR__ . '/var/file_mv.txt'))->content());
    }

    public function test_mv_rejects_mismatched_destination_scheme(): void
    {
        $this->expectException(InvalidSchemeException::class);

        native_local_filesystem()->mv(path(__DIR__ . '/var/a.txt'), path('memory:///var/b.txt'));
    }

    public function test_mv_rejects_mismatched_source_scheme(): void
    {
        $this->expectException(InvalidSchemeException::class);

        native_local_filesystem()->mv(path('memory:///var/a.txt'), path(__DIR__ . '/var/b.txt'));
    }

    public function test_not_removing_a_content_when_its_not_a_full_folder_path_pattern(): void
    {
        $fs = native_local_filesystem();

        $resource1 = fopen(__DIR__ . '/Fixtures/orders.csv', 'rb');
        static::assertIsResource($resource1);
        $fs->writeTo(path(__DIR__ . '/var/nested/orders/orders.txt'))->fromResource($resource1);

        $resource2 = fopen(__DIR__ . '/Fixtures/orders.csv', 'rb');
        static::assertIsResource($resource2);
        $fs->writeTo(path(__DIR__ . '/var/nested/orders/orders.csv'))->fromResource($resource2);

        $resource3 = fopen(__DIR__ . '/Fixtures/orders.csv', 'rb');
        static::assertIsResource($resource3);
        $fs->writeTo(path(__DIR__ . '/var/nested/orders/orders_01.csv'))->fromResource($resource3);

        $status1 = $fs->status(path(__DIR__ . '/var/nested/orders/orders.csv'));
        static::assertNotNull($status1);
        static::assertTrue($status1->isFile());

        $status2 = $fs->status(path(__DIR__ . '/var/nested/orders/orders_01.csv'));
        static::assertNotNull($status2);
        static::assertTrue($status2->isFile());

        static::assertFalse($fs->rm(path(__DIR__ . '/var/nested/orders/ord')));
    }

    public function test_open_file_stream_for_existing_file(): void
    {
        $stream = native_local_filesystem()->readFrom(path(__FILE__));

        $fileContent = file_get_contents(__FILE__);
        static::assertIsString($fileContent);
        static::assertSame(mb_substr($fileContent, 0, 100), $stream->read(100, 0));
    }

    public function test_open_file_stream_for_non_existing_file(): void
    {
        $path = __DIR__ . '/var/file.txt';

        $stream = native_local_filesystem()->writeTo(path($path));

        static::assertInstanceOf(NativeLocalDestinationStream::class, $stream);
    }

    public function test_read_from_rejects_mismatched_scheme(): void
    {
        $this->expectException(InvalidSchemeException::class);

        native_local_filesystem()->readFrom(path('memory:///var/foo.txt'));
    }

    public function test_reading_multi_partitioned_path(): void
    {
        $paths = iterator_to_array(native_local_filesystem()->list(
            path(__DIR__ . '/Fixtures/multi_partitions/**/*.txt'),
            new ScalarFunctionFilter(
                all(
                    ref('country')->equals(lit('pl')),
                    all(
                        ref('date')->cast('date')->greaterThanEqual(lit(new DateTimeImmutable('2022-01-02'))),
                        ref('date')->cast('date')->lessThan(lit(new DateTimeImmutable('2022-01-04'))),
                    ),
                ),
                schema(),
                flow_context(),
            ),
        ));

        $path1 = path(__DIR__ . '/Fixtures/multi_partitions/date=2022-01-02/country=pl/file.txt');
        $path1->partitions();
        $path2 = path(__DIR__ . '/Fixtures/multi_partitions/date=2022-01-03/country=pl/file.txt');
        $path2->partitions();

        $uris = array_map(static fn(FileStatus $s): string => $s->path->uri(), $paths);
        static::assertSame([$path1->uri(), $path2->uri()], $uris);
    }

    public function test_reading_partitioned_folder(): void
    {
        $statuses = iterator_to_array(native_local_filesystem()->list(
            path(__DIR__ . '/Fixtures/partitioned/**/*.txt'),
            new KeepAll(),
        ));

        $uris = array_map(static fn(FileStatus $s): string => $s->path->uri(), $statuses);
        static::assertSame(
            [
                path(__DIR__ . '/Fixtures/partitioned/partition_01=a/file_01.txt')->uri(),
                path(__DIR__ . '/Fixtures/partitioned/partition_01=b/file_02.txt')->uri(),
            ],
            $uris,
        );
    }

    public function test_reading_partitioned_folder_with_partitions_filtering(): void
    {
        $path = path(__DIR__ . '/Fixtures/partitioned/partition_01=b/file_02.txt');
        $path->partitions();

        $statuses = iterator_to_array(native_local_filesystem()->list(
            path(__DIR__ . '/Fixtures/partitioned/**/*.txt'),
            new ScalarFunctionFilter(ref('partition_01')->equals(lit('b')), schema(), flow_context()),
        ));

        $uris = array_map(static fn(FileStatus $s): string => $s->path->uri(), $statuses);
        static::assertSame([$path->uri()], $uris);
    }

    public function test_reading_partitioned_folder_with_pattern(): void
    {
        $statuses = iterator_to_array(native_local_filesystem()->list(
            path(__DIR__ . '/Fixtures/partitioned/partition_01=*/*.txt'),
            new KeepAll(),
        ));

        $uris = array_map(static fn(FileStatus $s): string => $s->path->uri(), $statuses);
        static::assertSame(
            [
                path(__DIR__ . '/Fixtures/partitioned/partition_01=a/file_01.txt')->uri(),
                path(__DIR__ . '/Fixtures/partitioned/partition_01=b/file_02.txt')->uri(),
            ],
            $uris,
        );
    }

    public function test_remove_directory_with_content_when_exists(): void
    {
        $fs = native_local_filesystem();

        $dirPath = path_real(__DIR__ . '/var/flow-fs-test-directory/');

        $stream = $fs->writeTo(path_real($dirPath->path() . '/remove_file_when_exists.txt'));
        $stream->append('some data to make file not empty');
        $stream->close();

        $dirStatus = $fs->status($dirPath);
        static::assertNotNull($dirStatus);
        static::assertTrue($dirStatus->isDirectory());

        $fileStatus = $fs->status($stream->path());
        static::assertNotNull($fileStatus);
        static::assertTrue($fileStatus->isFile());
    }

    public function test_remove_file_when_exists(): void
    {
        $fs = native_local_filesystem();

        $stream = $fs->writeTo(path_real(__DIR__ . '/var/flow-fs-test/remove_file_when_exists.txt'));
        $stream->append('some data to make file not empty');
        $stream->close();

        $status = $fs->status($stream->path());
        static::assertNotNull($status);
        static::assertTrue($status->isFile());

        static::assertTrue($fs->rm($stream->path()));

        static::assertNull($fs->status($stream->path()));
    }

    public function test_remove_pattern(): void
    {
        $fs = native_local_filesystem();

        $dirPath = path_real(__DIR__ . '/var/flow-fs-test-directory/');

        $stream = $fs->writeTo(path_real($dirPath->path() . '/remove_file_when_exists.txt'));
        $stream->append('some data to make file not empty');
        $stream->close();

        $dirStatus = $fs->status($dirPath);
        static::assertNotNull($dirStatus);
        static::assertTrue($dirStatus->isDirectory());

        $fileStatus = $fs->status($stream->path());
        static::assertNotNull($fileStatus);
        static::assertTrue($fileStatus->isFile());

        $fs->rm(path_real($dirPath->path() . '/*.txt'));

        $dirStatusAfter = $fs->status($dirPath);
        static::assertNotNull($dirStatusAfter);
        static::assertTrue($dirStatusAfter->isDirectory());
        static::assertNull($fs->status($stream->path()));
        $fs->rm($dirPath);
    }

    public function test_removing_folder(): void
    {
        $fs = native_local_filesystem();

        $resource1 = fopen(__DIR__ . '/Fixtures/orders.csv', 'rb');
        static::assertIsResource($resource1);
        $fs->writeTo(path(__DIR__ . '/var/orders.csv'))->fromResource($resource1);

        $resource2 = fopen(__DIR__ . '/Fixtures/orders.csv', 'rb');
        static::assertIsResource($resource2);
        $fs->writeTo(path(__DIR__ . '/var/nested/orders/orders.csv'))->fromResource($resource2);

        $resource3 = fopen(__DIR__ . '/Fixtures/orders.csv', 'rb');
        static::assertIsResource($resource3);
        $fs->writeTo(path(__DIR__ . '/var/nested/orders/orders_01.csv'))->fromResource($resource3);

        $status1 = $fs->status(path(__DIR__ . '/var/nested/orders/orders.csv'));
        static::assertNotNull($status1);
        static::assertTrue($status1->isFile());

        $status2 = $fs->status(path(__DIR__ . '/var/nested/orders/orders_01.csv'));
        static::assertNotNull($status2);
        static::assertTrue($status2->isFile());

        $fs->rm(path(__DIR__ . '/var/nested/orders'));

        $status3 = $fs->status(path(__DIR__ . '/var/orders.csv'));
        static::assertNotNull($status3);
        static::assertTrue($status3->isFile());
        static::assertNull($fs->status(path(__DIR__ . '/var/nested/orders/orders.csv')));
        static::assertNull($fs->status(path(__DIR__ . '/var/nested/orders/orders_01.csv')));
    }

    public function test_removing_folder_pattern(): void
    {
        $fs = native_local_filesystem();

        $resource1 = fopen(__DIR__ . '/Fixtures/orders.csv', 'rb');
        static::assertIsResource($resource1);
        $fs->writeTo(path(__DIR__ . '/var/nested/orders/orders.txt'))->fromResource($resource1);

        $resource2 = fopen(__DIR__ . '/Fixtures/orders.csv', 'rb');
        static::assertIsResource($resource2);
        $fs->writeTo(path(__DIR__ . '/var/nested/orders/orders.csv'))->fromResource($resource2);

        $resource3 = fopen(__DIR__ . '/Fixtures/orders.csv', 'rb');
        static::assertIsResource($resource3);
        $fs->writeTo(path(__DIR__ . '/var/nested/orders/orders_01.csv'))->fromResource($resource3);

        $status1 = $fs->status(path(__DIR__ . '/var/nested/orders/orders.csv'));
        static::assertNotNull($status1);
        static::assertTrue($status1->isFile());

        $status2 = $fs->status(path(__DIR__ . '/var/nested/orders/orders_01.csv'));
        static::assertNotNull($status2);
        static::assertTrue($status2->isFile());

        $fs->rm(path(__DIR__ . '/var/nested/orders/*.csv'));

        $status3 = $fs->status(path(__DIR__ . '/var/nested/orders/orders.txt'));
        static::assertNotNull($status3);
        static::assertTrue($status3->isFile());
        static::assertNull($fs->status(path(__DIR__ . '/var/nested/orders/orders.csv')));
        static::assertNull($fs->status(path(__DIR__ . '/var/nested/orders/orders_01.csv')));
    }

    public function test_removing_pattern_with_redundant_slashes(): void
    {
        $fs = native_local_filesystem();

        $resource = fopen(__DIR__ . '/Fixtures/orders.csv', 'rb');
        static::assertIsResource($resource);
        $fs->writeTo(path(__DIR__ . '/var/redundant_slash/file.txt'))->fromResource($resource);

        $fs->rm(path(__DIR__ . '/var/redundant_slash//*.txt'));

        static::assertNull($fs->status(path(__DIR__ . '/var/redundant_slash/file.txt')));
    }

    public function test_removing_recursive_pattern_with_nested_directories(): void
    {
        $fs = native_local_filesystem();

        $resource1 = fopen(__DIR__ . '/Fixtures/orders.csv', 'rb');
        static::assertIsResource($resource1);
        $fs->writeTo(path(__DIR__ . '/var/recursive_rm/dir_a/file1.txt'))->fromResource($resource1);

        $resource2 = fopen(__DIR__ . '/Fixtures/orders.csv', 'rb');
        static::assertIsResource($resource2);
        $fs->writeTo(path(__DIR__ . '/var/recursive_rm/dir_a/nested/file2.txt'))->fromResource($resource2);

        $resource3 = fopen(__DIR__ . '/Fixtures/orders.csv', 'rb');
        static::assertIsResource($resource3);
        $fs->writeTo(path(__DIR__ . '/var/recursive_rm/dir_b/file3.txt'))->fromResource($resource3);

        $fs->rm(path(__DIR__ . '/var/recursive_rm/**/*'));

        static::assertNull($fs->status(path(__DIR__ . '/var/recursive_rm/dir_a/file1.txt')));
        static::assertNull($fs->status(path(__DIR__ . '/var/recursive_rm/dir_a/nested/file2.txt')));
        static::assertNull($fs->status(path(__DIR__ . '/var/recursive_rm/dir_b/file3.txt')));
    }

    public function test_rm_rejects_mismatched_scheme(): void
    {
        $this->expectException(InvalidSchemeException::class);

        native_local_filesystem()->rm(path('memory:///var/foo.txt'));
    }

    public function test_scan_yields_all_matching_files(): void
    {
        $statuses = iterator_to_array(native_local_filesystem()->list(path(__DIR__
        . '/Fixtures/multi_partitions/**/*.txt')));

        $uris = array_map(static fn(FileStatus $s): string => $s->path->uri(), $statuses);

        static::assertSame(
            [
                path(__DIR__ . '/Fixtures/multi_partitions/date=2022-01-01/country=de/file.txt')->uri(),
                path(__DIR__ . '/Fixtures/multi_partitions/date=2022-01-01/country=pl/file.txt')->uri(),
                path(__DIR__ . '/Fixtures/multi_partitions/date=2022-01-02/country=de/file.txt')->uri(),
                path(__DIR__ . '/Fixtures/multi_partitions/date=2022-01-02/country=pl/file.txt')->uri(),
                path(__DIR__ . '/Fixtures/multi_partitions/date=2022-01-03/country=de/file.txt')->uri(),
                path(__DIR__ . '/Fixtures/multi_partitions/date=2022-01-03/country=pl/file.txt')->uri(),
                path(__DIR__ . '/Fixtures/multi_partitions/date=2022-01-04/country=de/file.txt')->uri(),
                path(__DIR__ . '/Fixtures/multi_partitions/date=2022-01-04/country=pl/file.txt')->uri(),
                path(__DIR__ . '/Fixtures/multi_partitions/date=2022-01-05/country=de/file.txt')->uri(),
                path(__DIR__ . '/Fixtures/multi_partitions/date=2022-01-05/country=pl/file.txt')->uri(),
            ],
            $uris,
        );
    }

    public function test_status_rejects_mismatched_scheme(): void
    {
        $this->expectException(InvalidSchemeException::class);

        native_local_filesystem()->status(path('memory:///var/foo.txt'));
    }

    #[TestWith(['aws-s3://bucket/orders.csv'])]
    #[TestWith(['memory://orders.csv'])]
    #[TestWith(['stdout://orders.csv'])]
    public function test_local_filesystem_does_not_support_remote_paths(string $uri): void
    {
        static::assertFalse(native_local_filesystem()->supports(path($uri)));
    }

    #[TestWith(['/tmp/orders.csv'])]
    #[TestWith(['file:///tmp/orders.csv'])]
    #[TestWith(['orders.csv'])]
    #[TestWith(['./orders.csv'])]
    #[TestWith(['/tmp/date={date}/orders.csv'])]
    #[TestWith(['/tmp/**/*.csv'])]
    public function test_local_filesystem_supports_local_paths(string $uri): void
    {
        static::assertTrue(native_local_filesystem()->supports(path($uri)));
    }

    public function test_tmp_dir_status(): void
    {
        $fs = native_local_filesystem();

        $status = $fs->status($fs->getSystemTmpDir());
        static::assertNotNull($status);
        static::assertTrue($status->isDirectory());
    }

    public function test_write_to_rejects_mismatched_scheme(): void
    {
        $this->expectException(InvalidSchemeException::class);

        native_local_filesystem()->writeTo(path('memory:///var/foo.txt'));
    }

    public function test_write_to_tmp_dir(): void
    {
        $fs = native_local_filesystem();

        $stream = $fs->writeTo($fs->getSystemTmpDir()->suffix('file.txt'));
        $stream->append('Hello, World!');
        $stream->close();

        $status = $fs->status($fs->getSystemTmpDir()->suffix('file.txt'));
        static::assertNotNull($status);
        static::assertTrue($status->isFile());
        static::assertSame('Hello, World!', $fs->readFrom($fs->getSystemTmpDir()->suffix('file.txt'))->content());

        $fs->rm($fs->getSystemTmpDir()->suffix('file.txt'));
    }

    public function test_write_to_tmp_dir_as_to_a_file(): void
    {
        $fs = native_local_filesystem();

        $this->expectExceptionMessage('Cannot write to system tmp directory');

        $fs->writeTo($fs->getSystemTmpDir());
    }

    public function test_writing_to_file(): void
    {
        $fs = native_local_filesystem();

        $stream = $fs->writeTo(path(__DIR__ . '/var/file.txt'));
        $stream->append('Hello, World!');
        $stream->close();

        $status = $fs->status(path(__DIR__ . '/var/file.txt'));
        static::assertNotNull($status);
        static::assertTrue($status->isFile());
        static::assertFalse($status->isDirectory());
        static::assertSame('Hello, World!', $fs->readFrom(path(__DIR__ . '/var/file.txt'))->content());

        $fs->rm(path(__DIR__ . '/var/file.txt'));
    }

    public function test_writing_to_file_from_resources(): void
    {
        $fs = native_local_filesystem();

        $resource = fopen(__DIR__ . '/Fixtures/orders.csv', 'rb');
        static::assertIsResource($resource);

        $stream = $fs->writeTo(path(__DIR__ . '/var/orders.csv'));
        $stream->fromResource($resource);
        $stream->close();

        $status = $fs->status(path(__DIR__ . '/var/orders.csv'));
        static::assertNotNull($status);
        static::assertTrue($status->isFile());
        static::assertFalse($status->isDirectory());
        static::assertSame(
            file_get_contents(__DIR__ . '/Fixtures/orders.csv'),
            $fs->readFrom(path(__DIR__ . '/var/orders.csv'))->content(),
        );

        $fs->rm(path(__DIR__ . '/var/orders.csv'));
    }
}
