<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Integration;

use function Flow\ETL\DSL\{all, lit, ref};
use function Flow\Filesystem\DSL\native_local_filesystem;
use Flow\ETL\Filesystem\{ScalarFunctionFilter};
use Flow\ETL\PHP\Type\{AutoCaster, Caster};
use Flow\ETL\Row\Factory\NativeEntryFactory;
use Flow\Filesystem\Path\Filter\KeepAll;
use Flow\Filesystem\{FileStatus, Path, Stream\NativeLocalDestinationStream};

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

        $stream = $fs->writeTo(new Path(__DIR__ . '/var/file.txt'));
        $stream->append("This is first line\n");
        $stream->close();

        $stream = $fs->appendTo(new Path(__DIR__ . '/var/file.txt'));
        $stream->append("This is second line\n");
        $stream->close();

        self::assertTrue($fs->status(new Path(__DIR__ . '/var/file.txt'))->isFile());
        self::assertFalse($fs->status(new Path(__DIR__ . '/var/file.txt'))->isDirectory());
        self::assertSame(
            <<<'TXT'
This is first line
This is second line

TXT
            ,
            $fs->readFrom(new Path(__DIR__ . '/var/file.txt'))->content()
        );

        $fs->rm(new Path(__DIR__ . '/var/file.txt'));
    }

    public function test_dir_exists() : void
    {
        self::assertFalse((native_local_filesystem())->status(new Path(__DIR__))->isFile());
        self::assertTrue((native_local_filesystem())->status(new Path(__DIR__))->isDirectory());
        self::assertNull((native_local_filesystem())->status(new Path(__DIR__ . '/not_existing_directory')));
    }

    public function test_fie_exists() : void
    {
        self::assertTrue((native_local_filesystem())->status(new Path(__FILE__))->isFile());
        self::assertFalse((native_local_filesystem())->status(new Path(__FILE__))->isDirectory());
        self::assertNull((native_local_filesystem())->status(new Path(__DIR__ . '/not_existing_file.php')));
    }

    public function test_file_pattern_exists() : void
    {
        self::assertTrue((native_local_filesystem())->status(new Path(__DIR__ . '/**/*.txt'))->isFile());
        self::assertNull((native_local_filesystem())->status(new Path(__DIR__ . '/**/*.pdf')));
    }

    public function test_file_status_on_existing_file() : void
    {
        $fs = native_local_filesystem();

        $fs->writeTo(new Path(__DIR__ . '/var/file.txt'))->fromResource(\fopen(__DIR__ . '/Fixtures/orders.csv', 'rb'));

        self::assertTrue($fs->status(new Path(__DIR__ . '/var/file.txt'))->isFile());
    }

    public function test_file_status_on_existing_folder() : void
    {
        $fs = native_local_filesystem();

        $fs->writeTo(new Path(__DIR__ . '/var/nested/orders/orders.txt'))->fromResource(\fopen(__DIR__ . '/Fixtures/orders.csv', 'rb'));

        self::assertTrue($fs->status(new Path(__DIR__ . '/var/nested/orders'))->isDirectory());
        self::assertTrue($fs->status(new Path(__DIR__ . '/var/nested/orders/'))->isDirectory());
    }

    public function test_file_status_on_non_existing_file() : void
    {
        $fs = native_local_filesystem();

        self::assertNull($fs->status(new Path(__DIR__ . '/var/non-existing-file.txt')));
    }

    public function test_file_status_on_non_existing_folder() : void
    {
        $fs = native_local_filesystem();

        self::assertNull($fs->status(new Path(__DIR__ . '/var/non-existing-folder/')));
    }

    public function test_file_status_on_non_existing_pattern() : void
    {
        $fs = native_local_filesystem();

        self::assertNull($fs->status(new Path(__DIR__ . '/var/non-existing-folder/*')));
    }

    public function test_file_status_on_partial_path() : void
    {
        $fs = native_local_filesystem();

        $fs->writeTo(new Path(__DIR__ . '/var/some_path_to/file.txt'))->fromResource(\fopen(__DIR__ . '/Fixtures/orders.csv', 'rb'));

        self::assertNull($fs->status(new Path(__DIR__ . '/var/some_path')));
    }

    public function test_file_status_on_pattern() : void
    {
        $fs = native_local_filesystem();

        $fs->writeTo(new Path(__DIR__ . '/var/some_path_to/file.txt'))->fromResource(\fopen(__DIR__ . '/Fixtures/orders.csv', 'rb'));

        self::assertTrue($fs->status(new Path(__DIR__ . '/var/some_path_to/*.txt'))->isFile());
        self::assertSame(
            'file:/' . __DIR__ . '/var/some_path_to/file.txt',
            $fs->status(new Path(__DIR__ . '/var/some_path_to/*.txt'))->path->uri()
        );
    }

    public function test_file_status_on_root_folder() : void
    {
        $fs = native_local_filesystem();

        self::assertTrue($fs->status(new Path(__DIR__ . '/var/'))->isDirectory());
    }

    public function test_move_blob() : void
    {
        $fs = native_local_filesystem();

        $fs->writeTo(new Path(__DIR__ . '/var/file.txt'))->append('Hello, World!');

        $fs->mv(new Path(__DIR__ . '/var/file.txt'), new Path(__DIR__ . '/var/file_mv.txt'));

        self::assertNull($fs->status(new Path(__DIR__ . '/var/file.txt')));
        self::assertSame('Hello, World!', $fs->readFrom(new Path(__DIR__ . '/var/file_mv.txt'))->content());
    }

    public function test_not_removing_a_content_when_its_not_a_full_folder_path_pattern() : void
    {
        $fs = native_local_filesystem();

        $fs->writeTo(new Path(__DIR__ . '/var/nested/orders/orders.txt'))->fromResource(\fopen(__DIR__ . '/Fixtures/orders.csv', 'rb'));
        $fs->writeTo(new Path(__DIR__ . '/var/nested/orders/orders.csv'))->fromResource(\fopen(__DIR__ . '/Fixtures/orders.csv', 'rb'));
        $fs->writeTo(new Path(__DIR__ . '/var/nested/orders/orders_01.csv'))->fromResource(\fopen(__DIR__ . '/Fixtures/orders.csv', 'rb'));

        self::assertTrue($fs->status(new Path(__DIR__ . '/var/nested/orders/orders.csv'))->isFile());
        self::assertTrue($fs->status(new Path(__DIR__ . '/var/nested/orders/orders_01.csv'))->isFile());

        self::assertFalse($fs->rm(new Path(__DIR__ . '/var/nested/orders/ord')));
    }

    public function test_open_file_stream_for_existing_file() : void
    {
        $stream = (native_local_filesystem())->readFrom(new Path(__FILE__));

        self::assertIsString($stream->read(100, 0));
        self::assertSame(
            \mb_substr(\file_get_contents(__FILE__), 0, 100),
            $stream->read(100, 0)
        );
    }

    public function test_open_file_stream_for_non_existing_file() : void
    {
        $path = __DIR__ . '/var/file.txt';

        $stream = (native_local_filesystem())->writeTo(new Path($path));

        self::assertInstanceOf(NativeLocalDestinationStream::class, $stream);
    }

    public function test_reading_multi_partitioned_path() : void
    {
        $paths = \iterator_to_array(
            (native_local_filesystem())
                ->list(
                    new Path(__DIR__ . '/Fixtures/multi_partitions/**/*.txt'),
                    new ScalarFunctionFilter(
                        all(
                            ref('country')->equals(lit('pl')),
                            all(
                                ref('date')->cast('date')->greaterThanEqual(lit(new \DateTimeImmutable('2022-01-02'))),
                                ref('date')->cast('date')->lessThan(lit(new \DateTimeImmutable('2022-01-04')))
                            )
                        ),
                        new NativeEntryFactory(),
                        new AutoCaster(Caster::default())
                    )
                )
        );
        \sort($paths);

        $path1 = new Path(__DIR__ . '/Fixtures/multi_partitions/date=2022-01-02/country=pl/file.txt');
        $path1->partitions();
        $path2 = new Path(__DIR__ . '/Fixtures/multi_partitions/date=2022-01-03/country=pl/file.txt');
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
        $paths = \iterator_to_array((native_local_filesystem())->list(new Path(__DIR__ . '/Fixtures/partitioned/**/*.txt'), new KeepAll()));
        \sort($paths);

        self::assertEquals(
            [
                new FileStatus(new Path(__DIR__ . '/Fixtures/partitioned/partition_01=a/file_01.txt'), true),
                new FileStatus(new Path(__DIR__ . '/Fixtures/partitioned/partition_01=b/file_02.txt'), true),
            ],
            $paths
        );
    }

    public function test_reading_partitioned_folder_with_partitions_filtering() : void
    {
        $path = new Path(__DIR__ . '/Fixtures/partitioned/partition_01=b/file_02.txt');
        $path->partitions();

        self::assertEquals(
            [
                new FileStatus($path, true),
            ],
            \iterator_to_array(
                (native_local_filesystem())
                    ->list(
                        new Path(__DIR__ . '/Fixtures/partitioned/**/*.txt'),
                        new ScalarFunctionFilter(ref('partition_01')->equals(lit('b')), new NativeEntryFactory(), new AutoCaster(Caster::default()))
                    )
            )
        );
    }

    public function test_reading_partitioned_folder_with_pattern() : void
    {
        $paths = \iterator_to_array((native_local_filesystem())->list(new Path(__DIR__ . '/Fixtures/partitioned/partition_01=*/*.txt'), new KeepAll()));
        \sort($paths);

        self::assertEquals(
            [
                new FileStatus(new Path(__DIR__ . '/Fixtures/partitioned/partition_01=a/file_01.txt'), true),
                new FileStatus(new Path(__DIR__ . '/Fixtures/partitioned/partition_01=b/file_02.txt'), true),
            ],
            $paths
        );
    }

    public function test_remove_directory_with_content_when_exists() : void
    {
        $fs = native_local_filesystem();

        $dirPath = Path::realpath(__DIR__ . '/var/flow-fs-test-directory/');

        $stream = $fs->writeTo(Path::realpath($dirPath->path() . '/remove_file_when_exists.txt'));
        $stream->append('some data to make file not empty');
        $stream->close();

        self::assertTrue($fs->status($dirPath)->isDirectory());
        self::assertTrue($fs->status($stream->path())->isFile());
    }

    public function test_remove_file_when_exists() : void
    {
        $fs = native_local_filesystem();

        $stream = $fs->writeTo(Path::realpath(__DIR__ . '/var/flow-fs-test/remove_file_when_exists.txt'));
        $stream->append('some data to make file not empty');
        $stream->close();

        self::assertTrue($fs->status($stream->path())->isFile());

        self::assertTrue($fs->rm($stream->path()));

        self::assertNull($fs->status($stream->path()));
    }

    public function test_remove_pattern() : void
    {
        $fs = native_local_filesystem();

        $dirPath = Path::realpath(__DIR__ . '/var/flow-fs-test-directory/');

        $stream = $fs->writeTo(Path::realpath($dirPath->path() . '/remove_file_when_exists.txt'));
        $stream->append('some data to make file not empty');
        $stream->close();

        self::assertTrue($fs->status($dirPath)->isDirectory());
        self::assertTrue($fs->status($stream->path())->isFile());
        $fs->rm(Path::realpath($dirPath->path() . '/*.txt'));
        self::assertTrue($fs->status($dirPath)->isDirectory());
        self::assertNull($fs->status($stream->path()));
        $fs->rm($dirPath);
    }

    public function test_removing_folder() : void
    {
        $fs = native_local_filesystem();

        $fs->writeTo(new Path(__DIR__ . '/var/orders.csv'))->fromResource(\fopen(__DIR__ . '/Fixtures/orders.csv', 'rb'));
        $fs->writeTo(new Path(__DIR__ . '/var/nested/orders/orders.csv'))->fromResource(\fopen(__DIR__ . '/Fixtures/orders.csv', 'rb'));
        $fs->writeTo(new Path(__DIR__ . '/var/nested/orders/orders_01.csv'))->fromResource(\fopen(__DIR__ . '/Fixtures/orders.csv', 'rb'));

        self::assertTrue($fs->status(new Path(__DIR__ . '/var/nested/orders/orders.csv'))->isFile());
        self::assertTrue($fs->status(new Path(__DIR__ . '/var/nested/orders/orders_01.csv'))->isFile());

        $fs->rm(new Path(__DIR__ . '/var/nested/orders'));

        self::assertTrue($fs->status(new Path(__DIR__ . '/var/orders.csv'))->isFile());
        self::assertNull($fs->status(new Path(__DIR__ . '/var/nested/orders/orders.csv')));
        self::assertNull($fs->status(new Path(__DIR__ . '/var/nested/orders/orders_01.csv')));
    }

    public function test_removing_folder_pattern() : void
    {
        $fs = native_local_filesystem();

        $fs->writeTo(new Path(__DIR__ . '/var/nested/orders/orders.txt'))->fromResource(\fopen(__DIR__ . '/Fixtures/orders.csv', 'rb'));
        $fs->writeTo(new Path(__DIR__ . '/var/nested/orders/orders.csv'))->fromResource(\fopen(__DIR__ . '/Fixtures/orders.csv', 'rb'));
        $fs->writeTo(new Path(__DIR__ . '/var/nested/orders/orders_01.csv'))->fromResource(\fopen(__DIR__ . '/Fixtures/orders.csv', 'rb'));

        self::assertTrue($fs->status(new Path(__DIR__ . '/var/nested/orders/orders.csv'))->isFile());
        self::assertTrue($fs->status(new Path(__DIR__ . '/var/nested/orders/orders_01.csv'))->isFile());

        $fs->rm(new Path(__DIR__ . '/var/nested/orders/*.csv'));

        self::assertTrue($fs->status(new Path(__DIR__ . '/var/nested/orders/orders.txt'))->isFile());
        self::assertNull($fs->status(new Path(__DIR__ . '/var/nested/orders/orders.csv')));
        self::assertNull($fs->status(new Path(__DIR__ . '/var/nested/orders/orders_01.csv')));
    }

    public function test_that_scan_sort_files_by_path_names() : void
    {
        $paths = \iterator_to_array(
            (native_local_filesystem())
                ->list(
                    new Path(__DIR__ . '/Fixtures/multi_partitions/**/*.txt'),
                )
        );

        self::assertEquals(
            [
                new FileStatus(new Path(__DIR__ . '/Fixtures/multi_partitions/date=2022-01-01/country=de/file.txt'), true),
                new FileStatus(new Path(__DIR__ . '/Fixtures/multi_partitions/date=2022-01-01/country=pl/file.txt'), true),
                new FileStatus(new Path(__DIR__ . '/Fixtures/multi_partitions/date=2022-01-02/country=de/file.txt'), true),
                new FileStatus(new Path(__DIR__ . '/Fixtures/multi_partitions/date=2022-01-02/country=pl/file.txt'), true),
                new FileStatus(new Path(__DIR__ . '/Fixtures/multi_partitions/date=2022-01-03/country=de/file.txt'), true),
                new FileStatus(new Path(__DIR__ . '/Fixtures/multi_partitions/date=2022-01-03/country=pl/file.txt'), true),
                new FileStatus(new Path(__DIR__ . '/Fixtures/multi_partitions/date=2022-01-04/country=de/file.txt'), true),
                new FileStatus(new Path(__DIR__ . '/Fixtures/multi_partitions/date=2022-01-04/country=pl/file.txt'), true),
                new FileStatus(new Path(__DIR__ . '/Fixtures/multi_partitions/date=2022-01-05/country=de/file.txt'), true),
                new FileStatus(new Path(__DIR__ . '/Fixtures/multi_partitions/date=2022-01-05/country=pl/file.txt'), true),
            ],
            $paths
        );
    }

    public function test_tmp_dir() : void
    {
        $fs = native_local_filesystem();

        self::assertSame('file:/' . sys_get_temp_dir(), $fs->getSystemTmpDir()->uri());
    }

    public function test_tmp_dir_status() : void
    {
        $fs = native_local_filesystem();

        self::assertTrue($fs->status($fs->getSystemTmpDir())->isDirectory());
    }

    public function test_write_to_tmp_dir() : void
    {
        $fs = native_local_filesystem();

        $stream = $fs->writeTo($fs->getSystemTmpDir()->suffix('file.txt'));
        $stream->append('Hello, World!');
        $stream->close();

        self::assertTrue($fs->status($fs->getSystemTmpDir()->suffix('file.txt'))->isFile());
        self::assertSame('Hello, World!', $fs->readFrom($fs->getSystemTmpDir()->suffix('file.txt'))->content());

        $fs->rm($fs->getSystemTmpDir()->suffix('file.txt'));
    }

    public function test_write_to_tmp_dir_as_to_a_file() : void
    {
        $fs = native_local_filesystem();

        $this->expectExceptionMessage('Cannot write to system tmp directory');

        $fs->writeTo($fs->getSystemTmpDir());
    }

    public function test_writing_to_azure_blob_storage() : void
    {
        $fs = native_local_filesystem();

        $stream = $fs->writeTo(new Path(__DIR__ . '/var/file.txt'));
        $stream->append('Hello, World!');
        $stream->close();

        self::assertTrue($fs->status(new Path(__DIR__ . '/var/file.txt'))->isFile());
        self::assertFalse($fs->status(new Path(__DIR__ . '/var/file.txt'))->isDirectory());
        self::assertSame('Hello, World!', $fs->readFrom(new Path(__DIR__ . '/var/file.txt'))->content());

        $fs->rm(new Path(__DIR__ . '/var/file.txt'));
    }

    public function test_writing_to_to_azure_from_resources() : void
    {
        $fs = native_local_filesystem();

        $stream = $fs->writeTo(new Path(__DIR__ . '/var/orders.csv'));
        $stream->fromResource(\fopen(__DIR__ . '/Fixtures/orders.csv', 'rb'));
        $stream->close();

        self::assertTrue($fs->status(new Path(__DIR__ . '/var/orders.csv'))->isFile());
        self::assertFalse($fs->status(new Path(__DIR__ . '/var/orders.csv'))->isDirectory());
        self::assertSame(\file_get_contents(__DIR__ . '/Fixtures/orders.csv'), $fs->readFrom(new Path(__DIR__ . '/var/orders.csv'))->content());

        $fs->rm(new Path(__DIR__ . '/var/orders.csv'));
    }
}
