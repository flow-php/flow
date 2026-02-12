<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Integration;

use function Flow\ETL\DSL\{all, config, flow_context, lit, ref};
use function Flow\Filesystem\DSL\{native_local_filesystem, path_real};
use function Flow\Filesystem\DSL\path;
use Flow\ETL\Filesystem\ScalarFunctionFilter;
use Flow\Filesystem\{FileStatus, Stream\NativeLocalDestinationStream};
use Flow\Filesystem\Path\Filter\KeepAll;
use Flow\Types\Type\AutoCaster;

final class NativeLocalFilesystemTest extends NativeLocalFilesystemTestCase
{
    protected function setUp() : void
    {
        if (!\file_exists(__DIR__ . '/var')) {
            \mkdir(__DIR__ . '/var');
        }
    }

    public function test_appending_to_existing_blob() : void
    {
        $fs = native_local_filesystem();

        $stream = $fs->writeTo(path(__DIR__ . '/var/file.txt'));
        $stream->append("This is first line\n");
        $stream->close();

        $stream = $fs->appendTo(path(__DIR__ . '/var/file.txt'));
        $stream->append("This is second line\n");
        $stream->close();

        $status = $fs->status(path(__DIR__ . '/var/file.txt'));
        self::assertNotNull($status);
        self::assertTrue($status->isFile());
        self::assertFalse($status->isDirectory());
        self::assertSame(
            <<<'TXT'
This is first line
This is second line

TXT
            ,
            $fs->readFrom(path(__DIR__ . '/var/file.txt'))->content()
        );

        $fs->rm(path(__DIR__ . '/var/file.txt'));
    }

    public function test_dir_exists() : void
    {
        $status = (native_local_filesystem())->status(path(__DIR__));
        self::assertNotNull($status);
        self::assertFalse($status->isFile());
        self::assertTrue($status->isDirectory());
        self::assertNull((native_local_filesystem())->status(path(__DIR__ . '/not_existing_directory')));
    }

    public function test_fie_exists() : void
    {
        $status = (native_local_filesystem())->status(path(__FILE__));
        self::assertNotNull($status);
        self::assertTrue($status->isFile());
        self::assertFalse($status->isDirectory());
        self::assertNull((native_local_filesystem())->status(path(__DIR__ . '/not_existing_file.php')));
    }

    public function test_file_pattern_exists() : void
    {
        $status = (native_local_filesystem())->status(path(__DIR__ . '/**/*.txt'));
        self::assertNotNull($status);
        self::assertTrue($status->isFile());
        self::assertNull((native_local_filesystem())->status(path(__DIR__ . '/**/*.pdf')));
    }

    public function test_file_status_on_existing_file() : void
    {
        $fs = native_local_filesystem();

        $resource = \fopen(__DIR__ . '/Fixtures/orders.csv', 'rb');
        self::assertIsResource($resource);
        $fs->writeTo(path(__DIR__ . '/var/file.txt'))->fromResource($resource);

        $status = $fs->status(path(__DIR__ . '/var/file.txt'));
        self::assertNotNull($status);
        self::assertTrue($status->isFile());
    }

    public function test_file_status_on_existing_folder() : void
    {
        $fs = native_local_filesystem();

        $resource = \fopen(__DIR__ . '/Fixtures/orders.csv', 'rb');
        self::assertIsResource($resource);
        $fs->writeTo(path(__DIR__ . '/var/nested/orders/orders.txt'))->fromResource($resource);

        $status1 = $fs->status(path(__DIR__ . '/var/nested/orders'));
        self::assertNotNull($status1);
        self::assertTrue($status1->isDirectory());

        $status2 = $fs->status(path(__DIR__ . '/var/nested/orders/'));
        self::assertNotNull($status2);
        self::assertTrue($status2->isDirectory());
    }

    public function test_file_status_on_non_existing_file() : void
    {
        $fs = native_local_filesystem();

        self::assertNull($fs->status(path(__DIR__ . '/var/non-existing-file.txt')));
    }

    public function test_file_status_on_non_existing_folder() : void
    {
        $fs = native_local_filesystem();

        self::assertNull($fs->status(path(__DIR__ . '/var/non-existing-folder/')));
    }

    public function test_file_status_on_non_existing_pattern() : void
    {
        $fs = native_local_filesystem();

        self::assertNull($fs->status(path(__DIR__ . '/var/non-existing-folder/*')));
    }

    public function test_file_status_on_partial_path() : void
    {
        $fs = native_local_filesystem();

        $resource = \fopen(__DIR__ . '/Fixtures/orders.csv', 'rb');
        self::assertIsResource($resource);
        $fs->writeTo(path(__DIR__ . '/var/some_path_to/file.txt'))->fromResource($resource);

        self::assertNull($fs->status(path(__DIR__ . '/var/some_path')));
    }

    public function test_file_status_on_root_folder() : void
    {
        $fs = native_local_filesystem();

        $status = $fs->status(path(__DIR__ . '/var/'));
        self::assertNotNull($status);
        self::assertTrue($status->isDirectory());
    }

    public function test_move_blob() : void
    {
        $fs = native_local_filesystem();

        $fs->writeTo(path(__DIR__ . '/var/file.txt'))->append('Hello, World!');

        $fs->mv(path(__DIR__ . '/var/file.txt'), path(__DIR__ . '/var/file_mv.txt'));

        self::assertNull($fs->status(path(__DIR__ . '/var/file.txt')));
        self::assertSame('Hello, World!', $fs->readFrom(path(__DIR__ . '/var/file_mv.txt'))->content());
    }

    public function test_not_removing_a_content_when_its_not_a_full_folder_path_pattern() : void
    {
        $fs = native_local_filesystem();

        $resource1 = \fopen(__DIR__ . '/Fixtures/orders.csv', 'rb');
        self::assertIsResource($resource1);
        $fs->writeTo(path(__DIR__ . '/var/nested/orders/orders.txt'))->fromResource($resource1);

        $resource2 = \fopen(__DIR__ . '/Fixtures/orders.csv', 'rb');
        self::assertIsResource($resource2);
        $fs->writeTo(path(__DIR__ . '/var/nested/orders/orders.csv'))->fromResource($resource2);

        $resource3 = \fopen(__DIR__ . '/Fixtures/orders.csv', 'rb');
        self::assertIsResource($resource3);
        $fs->writeTo(path(__DIR__ . '/var/nested/orders/orders_01.csv'))->fromResource($resource3);

        $status1 = $fs->status(path(__DIR__ . '/var/nested/orders/orders.csv'));
        self::assertNotNull($status1);
        self::assertTrue($status1->isFile());

        $status2 = $fs->status(path(__DIR__ . '/var/nested/orders/orders_01.csv'));
        self::assertNotNull($status2);
        self::assertTrue($status2->isFile());

        self::assertFalse($fs->rm(path(__DIR__ . '/var/nested/orders/ord')));
    }

    public function test_open_file_stream_for_existing_file() : void
    {
        $stream = (native_local_filesystem())->readFrom(path(__FILE__));

        $fileContent = \file_get_contents(__FILE__);
        self::assertIsString($fileContent);
        self::assertSame(
            \mb_substr($fileContent, 0, 100),
            $stream->read(100, 0)
        );
    }

    public function test_open_file_stream_for_non_existing_file() : void
    {
        $path = __DIR__ . '/var/file.txt';

        $stream = (native_local_filesystem())->writeTo(path($path));

        self::assertInstanceOf(NativeLocalDestinationStream::class, $stream);
    }

    public function test_reading_multi_partitioned_path() : void
    {
        $paths = \iterator_to_array(
            (native_local_filesystem())
                ->list(
                    path(__DIR__ . '/Fixtures/multi_partitions/**/*.txt'),
                    new ScalarFunctionFilter(
                        all(
                            ref('country')->equals(lit('pl')),
                            all(
                                ref('date')->cast('date')->greaterThanEqual(lit(new \DateTimeImmutable('2022-01-02'))),
                                ref('date')->cast('date')->lessThan(lit(new \DateTimeImmutable('2022-01-04')))
                            )
                        ),
                        flow_context(config())->entryFactory(),
                        new AutoCaster(),
                        flow_context()
                    )
                )
        );
        \sort($paths);

        $path1 = path(__DIR__ . '/Fixtures/multi_partitions/date=2022-01-02/country=pl/file.txt');
        $path1->partitions();
        $path2 = path(__DIR__ . '/Fixtures/multi_partitions/date=2022-01-03/country=pl/file.txt');
        $path2->partitions();

        self::assertEquals(
            [
                new FileStatus($path1, true),
                new FileStatus($path2, true),
            ],
            $paths
        );
    }

    public function test_reading_partitioned_folder() : void
    {
        $paths = \iterator_to_array((native_local_filesystem())->list(path(__DIR__ . '/Fixtures/partitioned/**/*.txt'), new KeepAll()));
        \sort($paths);

        self::assertEquals(
            [
                new FileStatus(path(__DIR__ . '/Fixtures/partitioned/partition_01=a/file_01.txt'), true),
                new FileStatus(path(__DIR__ . '/Fixtures/partitioned/partition_01=b/file_02.txt'), true),
            ],
            $paths
        );
    }

    public function test_reading_partitioned_folder_with_partitions_filtering() : void
    {
        $path = path(__DIR__ . '/Fixtures/partitioned/partition_01=b/file_02.txt');
        $path->partitions();

        self::assertEquals(
            [
                new FileStatus($path, true),
            ],
            \iterator_to_array(
                (native_local_filesystem())
                    ->list(
                        path(__DIR__ . '/Fixtures/partitioned/**/*.txt'),
                        new ScalarFunctionFilter(ref('partition_01')->equals(lit('b')), flow_context(config())->entryFactory(), new AutoCaster(), flow_context())
                    )
            )
        );
    }

    public function test_reading_partitioned_folder_with_pattern() : void
    {
        $paths = \iterator_to_array((native_local_filesystem())->list(path(__DIR__ . '/Fixtures/partitioned/partition_01=*/*.txt'), new KeepAll()));
        \sort($paths);

        self::assertEquals(
            [
                new FileStatus(path(__DIR__ . '/Fixtures/partitioned/partition_01=a/file_01.txt'), true),
                new FileStatus(path(__DIR__ . '/Fixtures/partitioned/partition_01=b/file_02.txt'), true),
            ],
            $paths
        );
    }

    public function test_remove_directory_with_content_when_exists() : void
    {
        $fs = native_local_filesystem();

        $dirPath = path_real(__DIR__ . '/var/flow-fs-test-directory/');

        $stream = $fs->writeTo(path_real($dirPath->path() . '/remove_file_when_exists.txt'));
        $stream->append('some data to make file not empty');
        $stream->close();

        $dirStatus = $fs->status($dirPath);
        self::assertNotNull($dirStatus);
        self::assertTrue($dirStatus->isDirectory());

        $fileStatus = $fs->status($stream->path());
        self::assertNotNull($fileStatus);
        self::assertTrue($fileStatus->isFile());
    }

    public function test_remove_file_when_exists() : void
    {
        $fs = native_local_filesystem();

        $stream = $fs->writeTo(path_real(__DIR__ . '/var/flow-fs-test/remove_file_when_exists.txt'));
        $stream->append('some data to make file not empty');
        $stream->close();

        $status = $fs->status($stream->path());
        self::assertNotNull($status);
        self::assertTrue($status->isFile());

        self::assertTrue($fs->rm($stream->path()));

        self::assertNull($fs->status($stream->path()));
    }

    public function test_remove_pattern() : void
    {
        $fs = native_local_filesystem();

        $dirPath = path_real(__DIR__ . '/var/flow-fs-test-directory/');

        $stream = $fs->writeTo(path_real($dirPath->path() . '/remove_file_when_exists.txt'));
        $stream->append('some data to make file not empty');
        $stream->close();

        $dirStatus = $fs->status($dirPath);
        self::assertNotNull($dirStatus);
        self::assertTrue($dirStatus->isDirectory());

        $fileStatus = $fs->status($stream->path());
        self::assertNotNull($fileStatus);
        self::assertTrue($fileStatus->isFile());

        $fs->rm(path_real($dirPath->path() . '/*.txt'));

        $dirStatusAfter = $fs->status($dirPath);
        self::assertNotNull($dirStatusAfter);
        self::assertTrue($dirStatusAfter->isDirectory());
        self::assertNull($fs->status($stream->path()));
        $fs->rm($dirPath);
    }

    public function test_removing_folder() : void
    {
        $fs = native_local_filesystem();

        $resource1 = \fopen(__DIR__ . '/Fixtures/orders.csv', 'rb');
        self::assertIsResource($resource1);
        $fs->writeTo(path(__DIR__ . '/var/orders.csv'))->fromResource($resource1);

        $resource2 = \fopen(__DIR__ . '/Fixtures/orders.csv', 'rb');
        self::assertIsResource($resource2);
        $fs->writeTo(path(__DIR__ . '/var/nested/orders/orders.csv'))->fromResource($resource2);

        $resource3 = \fopen(__DIR__ . '/Fixtures/orders.csv', 'rb');
        self::assertIsResource($resource3);
        $fs->writeTo(path(__DIR__ . '/var/nested/orders/orders_01.csv'))->fromResource($resource3);

        $status1 = $fs->status(path(__DIR__ . '/var/nested/orders/orders.csv'));
        self::assertNotNull($status1);
        self::assertTrue($status1->isFile());

        $status2 = $fs->status(path(__DIR__ . '/var/nested/orders/orders_01.csv'));
        self::assertNotNull($status2);
        self::assertTrue($status2->isFile());

        $fs->rm(path(__DIR__ . '/var/nested/orders'));

        $status3 = $fs->status(path(__DIR__ . '/var/orders.csv'));
        self::assertNotNull($status3);
        self::assertTrue($status3->isFile());
        self::assertNull($fs->status(path(__DIR__ . '/var/nested/orders/orders.csv')));
        self::assertNull($fs->status(path(__DIR__ . '/var/nested/orders/orders_01.csv')));
    }

    public function test_removing_folder_pattern() : void
    {
        $fs = native_local_filesystem();

        $resource1 = \fopen(__DIR__ . '/Fixtures/orders.csv', 'rb');
        self::assertIsResource($resource1);
        $fs->writeTo(path(__DIR__ . '/var/nested/orders/orders.txt'))->fromResource($resource1);

        $resource2 = \fopen(__DIR__ . '/Fixtures/orders.csv', 'rb');
        self::assertIsResource($resource2);
        $fs->writeTo(path(__DIR__ . '/var/nested/orders/orders.csv'))->fromResource($resource2);

        $resource3 = \fopen(__DIR__ . '/Fixtures/orders.csv', 'rb');
        self::assertIsResource($resource3);
        $fs->writeTo(path(__DIR__ . '/var/nested/orders/orders_01.csv'))->fromResource($resource3);

        $status1 = $fs->status(path(__DIR__ . '/var/nested/orders/orders.csv'));
        self::assertNotNull($status1);
        self::assertTrue($status1->isFile());

        $status2 = $fs->status(path(__DIR__ . '/var/nested/orders/orders_01.csv'));
        self::assertNotNull($status2);
        self::assertTrue($status2->isFile());

        $fs->rm(path(__DIR__ . '/var/nested/orders/*.csv'));

        $status3 = $fs->status(path(__DIR__ . '/var/nested/orders/orders.txt'));
        self::assertNotNull($status3);
        self::assertTrue($status3->isFile());
        self::assertNull($fs->status(path(__DIR__ . '/var/nested/orders/orders.csv')));
        self::assertNull($fs->status(path(__DIR__ . '/var/nested/orders/orders_01.csv')));
    }

    public function test_that_scan_sort_files_by_path_names() : void
    {
        $paths = \iterator_to_array(
            (native_local_filesystem())
                ->list(
                    path(__DIR__ . '/Fixtures/multi_partitions/**/*.txt'),
                )
        );

        self::assertEquals(
            [
                new FileStatus(path(__DIR__ . '/Fixtures/multi_partitions/date=2022-01-01/country=de/file.txt'), true),
                new FileStatus(path(__DIR__ . '/Fixtures/multi_partitions/date=2022-01-01/country=pl/file.txt'), true),
                new FileStatus(path(__DIR__ . '/Fixtures/multi_partitions/date=2022-01-02/country=de/file.txt'), true),
                new FileStatus(path(__DIR__ . '/Fixtures/multi_partitions/date=2022-01-02/country=pl/file.txt'), true),
                new FileStatus(path(__DIR__ . '/Fixtures/multi_partitions/date=2022-01-03/country=de/file.txt'), true),
                new FileStatus(path(__DIR__ . '/Fixtures/multi_partitions/date=2022-01-03/country=pl/file.txt'), true),
                new FileStatus(path(__DIR__ . '/Fixtures/multi_partitions/date=2022-01-04/country=de/file.txt'), true),
                new FileStatus(path(__DIR__ . '/Fixtures/multi_partitions/date=2022-01-04/country=pl/file.txt'), true),
                new FileStatus(path(__DIR__ . '/Fixtures/multi_partitions/date=2022-01-05/country=de/file.txt'), true),
                new FileStatus(path(__DIR__ . '/Fixtures/multi_partitions/date=2022-01-05/country=pl/file.txt'), true),
            ],
            $paths
        );
    }

    public function test_tmp_dir_status() : void
    {
        $fs = native_local_filesystem();

        $status = $fs->status($fs->getSystemTmpDir());
        self::assertNotNull($status);
        self::assertTrue($status->isDirectory());
    }

    public function test_write_to_tmp_dir() : void
    {
        $fs = native_local_filesystem();

        $stream = $fs->writeTo($fs->getSystemTmpDir()->suffix('file.txt'));
        $stream->append('Hello, World!');
        $stream->close();

        $status = $fs->status($fs->getSystemTmpDir()->suffix('file.txt'));
        self::assertNotNull($status);
        self::assertTrue($status->isFile());
        self::assertSame('Hello, World!', $fs->readFrom($fs->getSystemTmpDir()->suffix('file.txt'))->content());

        $fs->rm($fs->getSystemTmpDir()->suffix('file.txt'));
    }

    public function test_write_to_tmp_dir_as_to_a_file() : void
    {
        $fs = native_local_filesystem();

        $this->expectExceptionMessage('Cannot write to system tmp directory');

        $fs->writeTo($fs->getSystemTmpDir());
    }

    public function test_writing_to_file() : void
    {
        $fs = native_local_filesystem();

        $stream = $fs->writeTo(path(__DIR__ . '/var/file.txt'));
        $stream->append('Hello, World!');
        $stream->close();

        $status = $fs->status(path(__DIR__ . '/var/file.txt'));
        self::assertNotNull($status);
        self::assertTrue($status->isFile());
        self::assertFalse($status->isDirectory());
        self::assertSame('Hello, World!', $fs->readFrom(path(__DIR__ . '/var/file.txt'))->content());

        $fs->rm(path(__DIR__ . '/var/file.txt'));
    }

    public function test_writing_to_file_from_resources() : void
    {
        $fs = native_local_filesystem();

        $resource = \fopen(__DIR__ . '/Fixtures/orders.csv', 'rb');
        self::assertIsResource($resource);

        $stream = $fs->writeTo(path(__DIR__ . '/var/orders.csv'));
        $stream->fromResource($resource);
        $stream->close();

        $status = $fs->status(path(__DIR__ . '/var/orders.csv'));
        self::assertNotNull($status);
        self::assertTrue($status->isFile());
        self::assertFalse($status->isDirectory());
        self::assertSame(\file_get_contents(__DIR__ . '/Fixtures/orders.csv'), $fs->readFrom(path(__DIR__ . '/var/orders.csv'))->content());

        $fs->rm(path(__DIR__ . '/var/orders.csv'));
    }
}
