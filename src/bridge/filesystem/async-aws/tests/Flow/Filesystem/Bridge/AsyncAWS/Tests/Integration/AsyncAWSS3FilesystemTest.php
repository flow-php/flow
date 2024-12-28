<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\AsyncAWS\Tests\Integration;

use function Flow\Filesystem\Bridge\AsyncAWS\DSL\{aws_s3_filesystem};
use function Flow\Filesystem\DSL\path;
use Flow\Filesystem\FileStatus;

final class AsyncAWSS3FilesystemTest extends AsyncAWSS3TestCase
{
    public function test_appending_to_existing_5mb_blob() : void
    {
        $fs = aws_s3_filesystem($this->bucket(), $this->s3Client());

        $fs->writeTo(path('aws-s3://var/file.txt'))
            ->append(\str_repeat('a', 1024 * 1024 * 5))
            ->close();

        $fs->appendTo(path('aws-s3://var/file.txt'))
            ->append("This is second line\n")
            ->close();

        self::assertTrue($fs->status(path('aws-s3://var/file.txt'))->isFile());
        self::assertFalse($fs->status(path('aws-s3://var/file.txt'))->isDirectory());

        self::assertStringStartsWith(\str_repeat('a', 1024), $fs->readFrom(path('aws-s3://var/file.txt'))->read(1024, 0));
        self::assertStringEndsWith("This is second line\n", $fs->readFrom(path('aws-s3://var/file.txt'))->read(58, 1024 * 1024 * 5));

        $fs->rm(path('aws-s3://var/file.txt'));
    }

    public function test_appending_to_existing_blob() : void
    {
        $fs = aws_s3_filesystem($this->bucket(), $this->s3Client());

        $fs->writeTo(path('aws-s3://var/file.txt'))
            ->append("This is first line\n")
            ->close();

        $fs->appendTo(path('aws-s3://var/file.txt'))
            ->append("This is second line\n")
            ->close();

        self::assertTrue($fs->status(path('aws-s3://var/file.txt'))->isFile());
        self::assertFalse($fs->status(path('aws-s3://var/file.txt'))->isDirectory());
        self::assertSame(
            <<<'TXT'
This is first line
This is second line

TXT
            ,
            $fs->readFrom(path('aws-s3://var/file.txt'))->content()
        );

        $fs->rm(path('aws-s3://var/file.txt'));
    }

    public function test_file_status_on_existing_file() : void
    {
        $fs = aws_s3_filesystem($this->bucket(), $this->s3Client());

        $stream = $fs->writeTo(path('aws-s3://orders.csv'))->fromResource(\fopen(__DIR__ . '/Fixtures/orders.csv', 'rb'));
        $stream->close();

        self::assertTrue($fs->status(path('aws-s3://orders.csv'))->isFile());
    }

    public function test_file_status_on_existing_folder() : void
    {
        $fs = aws_s3_filesystem($this->bucket(), $this->s3Client());

        $fs->writeTo(path('aws-s3://var/nested/orders.csv'))
            ->fromResource(\fopen(__DIR__ . '/Fixtures/orders.csv', 'rb'))
            ->close();

        self::assertTrue($fs->status(path('aws-s3://var/nested'))->isDirectory());
        self::assertTrue($fs->status(path('aws-s3://var/nested/'))->isDirectory());
    }

    public function test_file_status_on_non_existing_file() : void
    {
        $fs = aws_s3_filesystem($this->bucket(), $this->s3Client());

        self::assertNull($fs->status(path('aws-s3://var/nested/orders.csv')));
    }

    public function test_file_status_on_non_existing_folder() : void
    {
        $fs = aws_s3_filesystem($this->bucket(), $this->s3Client());

        self::assertNull($fs->status(path('aws-s3://var/non-existing-folder/')));
    }

    public function test_file_status_on_non_existing_pattern() : void
    {
        $fs = aws_s3_filesystem($this->bucket(), $this->s3Client());

        self::assertNull($fs->status(path('aws-s3://var/non-existing-folder/*')));
    }

    public function test_file_status_on_partial_path() : void
    {
        $fs = aws_s3_filesystem($this->bucket(), $this->s3Client());

        $stream = $fs->writeTo(path('aws-s3://var/some_path_to/file.txt'))->fromResource(\fopen(__DIR__ . '/Fixtures/orders.csv', 'rb'));
        $stream->close();

        self::assertNull($fs->status(path('aws-s3://var/some_path')));
    }

    public function test_file_status_on_pattern() : void
    {
        $fs = aws_s3_filesystem($this->bucket(), $this->s3Client());

        $stream = $fs->writeTo(path('aws-s3://var/some_path_to/file.txt'))->fromResource(\fopen(__DIR__ . '/Fixtures/orders.csv', 'rb'));
        $stream->close();

        self::assertTrue($fs->status(path('aws-s3://var/some_path_to/*.txt'))->isFile());
        self::assertSame(
            'aws-s3://var/some_path_to/file.txt',
            $fs->status(path('aws-s3://var/some_path_to/*.txt'))->path->uri()
        );
    }

    public function test_file_status_on_root_folder() : void
    {
        $fs = aws_s3_filesystem($this->bucket(), $this->s3Client());

        self::assertTrue($fs->status(path('aws-s3:///'))->isDirectory());
    }

    public function test_move_blob() : void
    {
        $fs = aws_s3_filesystem($this->bucket(), $this->s3Client());

        $fs->writeTo(path('aws-s3://var/file.txt'))->append('Hello, World!')->close();

        $fs->mv(path('aws-s3://var/file.txt'), path('aws-s3://var/file_mv.txt'));

        self::assertNull($fs->status(path('aws-s3://var/file.txt')));
        self::assertSame('Hello, World!', $fs->readFrom(path('aws-s3://var/file_mv.txt'))->content());
    }

    public function test_not_removing_a_content_when_its_not_a_full_folder_path_pattern() : void
    {
        $fs = aws_s3_filesystem($this->bucket(), $this->s3Client());

        $fs->writeTo(path('aws-s3://var/nested/orders/orders.txt'))->fromResource(\fopen(__DIR__ . '/Fixtures/orders.csv', 'rb'))->close();
        $fs->writeTo(path('aws-s3://var/nested/orders/orders.csv'))->fromResource(\fopen(__DIR__ . '/Fixtures/orders.csv', 'rb'))->close();
        $fs->writeTo(path('aws-s3://var/nested/orders/orders_01.csv'))->fromResource(\fopen(__DIR__ . '/Fixtures/orders.csv', 'rb'))->close();

        self::assertTrue($fs->status(path('aws-s3://var/nested/orders/orders.csv'))->isFile());
        self::assertTrue($fs->status(path('aws-s3://var/nested/orders/orders_01.csv'))->isFile());

        self::assertFalse($fs->rm(path('aws-s3://var/nested/orders/ord')));
    }

    public function test_remove_file_when_exists() : void
    {
        $fs = aws_s3_filesystem($this->bucket(), $this->s3Client());

        $stream = $fs->writeTo(path('aws-s3://var/flow-fs-test/remove_file_when_exists.txt'));
        $stream->append('some data to make file not empty');
        $stream->close();

        self::assertTrue($fs->status($stream->path())->isFile());

        self::assertTrue($fs->rm($stream->path()));
        self::assertNull($fs->status($stream->path()));
    }

    public function test_remove_pattern() : void
    {
        $fs = aws_s3_filesystem($this->bucket(), $this->s3Client());

        $fs->writeTo(path('aws-s3://var/flow-fs-test-directory/remove_file_when_exists.txt'))
            ->append('some data to make file not empty')
            ->close();
        $fs->writeTo(path('aws-s3://var/flow-fs-test-directory/remove_file_when_exists.md'))
            ->append('some data to make file not empty')
            ->close();

        self::assertTrue($fs->status(path('aws-s3://var/flow-fs-test-directory/'))->isDirectory());
        self::assertTrue($fs->status(path('aws-s3://var/flow-fs-test-directory/remove_file_when_exists.txt'))->isFile());
        $fs->rm(path('aws-s3://var/flow-fs-test-directory/*.txt'));
        self::assertTrue($fs->status(path('aws-s3://var/flow-fs-test-directory/'))->isDirectory());
        self::assertNull($fs->status(path('aws-s3://var/flow-fs-test-directory/remove_file_when_exists.txt')));
        $fs->rm(path('aws-s3://var/flow-fs-test-directory/'));
    }

    public function test_removing_folder() : void
    {
        $fs = aws_s3_filesystem($this->bucket(), $this->s3Client());

        $fs->writeTo(path('aws-s3://var/orders.csv'))->fromResource(\fopen(__DIR__ . '/Fixtures/orders.csv', 'rb'))->close();
        $fs->writeTo(path('aws-s3://var/nested/orders/orders.csv'))->fromResource(\fopen(__DIR__ . '/Fixtures/orders.csv', 'rb'))->close();
        $fs->writeTo(path('aws-s3://var/nested/orders/orders_01.csv'))->fromResource(\fopen(__DIR__ . '/Fixtures/orders.csv', 'rb'))->close();

        self::assertTrue($fs->status(path('aws-s3://var/nested/orders/orders.csv'))->isFile());
        self::assertTrue($fs->status(path('aws-s3://var/nested/orders/orders_01.csv'))->isFile());

        $fs->rm(path('aws-s3://var/nested/orders'));

        self::assertTrue($fs->status(path('aws-s3://var/orders.csv'))->isFile());
        self::assertNull($fs->status(path('aws-s3://var/nested/orders/orders.csv')));
        self::assertNull($fs->status(path('aws-s3://var/nested/orders/orders_01.csv')));
    }

    public function test_removing_folder_pattern() : void
    {
        $fs = aws_s3_filesystem($this->bucket(), $this->s3Client());

        $fs->writeTo(path('aws-s3://var/nested/orders/orders.txt'))->fromResource(\fopen(__DIR__ . '/Fixtures/orders.csv', 'rb'))->close();
        $fs->writeTo(path('aws-s3://var/nested/orders/orders.csv'))->fromResource(\fopen(__DIR__ . '/Fixtures/orders.csv', 'rb'))->close();
        $fs->writeTo(path('aws-s3://var/nested/orders/orders_01.csv'))->fromResource(\fopen(__DIR__ . '/Fixtures/orders.csv', 'rb'))->close();

        self::assertTrue($fs->status(path('aws-s3://var/nested/orders/orders.csv'))->isFile());
        self::assertTrue($fs->status(path('aws-s3://var/nested/orders/orders_01.csv'))->isFile());

        $fs->rm(path('aws-s3://var/nested/orders/*.csv'));

        self::assertTrue($fs->status(path('aws-s3://var/nested/orders/orders.txt'))->isFile());
        self::assertNull($fs->status(path('aws-s3://var/nested/orders/orders.csv')));
        self::assertNull($fs->status(path('aws-s3://var/nested/orders/orders_01.csv')));
    }

    public function test_that_scan_sort_files_by_path_names() : void
    {
        $fs = aws_s3_filesystem($this->bucket(), $this->s3Client());

        $fs->writeTo(path('aws-s3://multi_partitions/date=2022-01-01/country=de/file.txt'))->append('test')->close();
        $fs->writeTo(path('aws-s3://multi_partitions/date=2022-01-01/country=pl/file.txt'))->append('test')->close();
        $fs->writeTo(path('aws-s3://multi_partitions/date=2022-01-02/country=de/file.txt'))->append('test')->close();
        $fs->writeTo(path('aws-s3://multi_partitions/date=2022-01-02/country=pl/file.txt'))->append('test')->close();
        $fs->writeTo(path('aws-s3://multi_partitions/date=2022-01-03/country=de/file.txt'))->append('test')->close();
        $fs->writeTo(path('aws-s3://multi_partitions/date=2022-01-03/country=pl/file.txt'))->append('test')->close();
        $fs->writeTo(path('aws-s3://multi_partitions/date=2022-01-04/country=de/file.txt'))->append('test')->close();
        $fs->writeTo(path('aws-s3://multi_partitions/date=2022-01-04/country=pl/file.txt'))->append('test')->close();
        $fs->writeTo(path('aws-s3://multi_partitions/date=2022-01-05/country=de/file.txt'))->append('test')->close();
        $fs->writeTo(path('aws-s3://multi_partitions/date=2022-01-05/country=pl/file.txt'))->append('test')->close();

        $paths = \iterator_to_array($fs->list(path('aws-s3://multi_partitions/**/*.txt')));

        self::assertTrue($fs->status(path('aws-s3://multi_partitions/**/*.txt'))->isFile());

        self::assertEquals(
            [
                new FileStatus(path('aws-s3://multi_partitions/date=2022-01-01/country=de/file.txt'), true),
                new FileStatus(path('aws-s3://multi_partitions/date=2022-01-01/country=pl/file.txt'), true),
                new FileStatus(path('aws-s3://multi_partitions/date=2022-01-02/country=de/file.txt'), true),
                new FileStatus(path('aws-s3://multi_partitions/date=2022-01-02/country=pl/file.txt'), true),
                new FileStatus(path('aws-s3://multi_partitions/date=2022-01-03/country=de/file.txt'), true),
                new FileStatus(path('aws-s3://multi_partitions/date=2022-01-03/country=pl/file.txt'), true),
                new FileStatus(path('aws-s3://multi_partitions/date=2022-01-04/country=de/file.txt'), true),
                new FileStatus(path('aws-s3://multi_partitions/date=2022-01-04/country=pl/file.txt'), true),
                new FileStatus(path('aws-s3://multi_partitions/date=2022-01-05/country=de/file.txt'), true),
                new FileStatus(path('aws-s3://multi_partitions/date=2022-01-05/country=pl/file.txt'), true),
            ],
            $paths
        );
    }

    public function test_tmp_dir() : void
    {
        $fs = aws_s3_filesystem($this->bucket(), $this->s3Client());

        self::assertSame('aws-s3://_$azure_flow_tmp$/', $fs->getSystemTmpDir()->uri());
    }

    public function test_tmp_dir_status() : void
    {
        $fs = aws_s3_filesystem($this->bucket(), $this->s3Client());

        self::assertTrue($fs->status($fs->getSystemTmpDir())->isDirectory());
    }

    public function test_write_to_tmp_dir() : void
    {
        $fs = aws_s3_filesystem($this->bucket(), $this->s3Client());

        $stream = $fs->writeTo($filePath = $fs->getSystemTmpDir()->suffix('file.txt'));
        $stream->append('Hello, World!');
        $stream->close();

        self::assertTrue($fs->status($filePath)->isFile());
        self::assertSame('Hello, World!', $fs->readFrom($filePath)->content());

        $fs->rm($filePath);
    }

    public function test_write_to_tmp_dir_as_to_a_file() : void
    {
        $fs = aws_s3_filesystem($this->bucket(), $this->s3Client());

        $this->expectExceptionMessage('Cannot write to system tmp directory');

        $fs->writeTo($fs->getSystemTmpDir());
    }

    public function test_writing_to_aws_s3_storage() : void
    {
        $fs = aws_s3_filesystem($this->bucket(), $this->s3Client());

        $stream = $fs->writeTo(path('aws-s3://file.txt'));
        $stream->append('Hello, World!');
        $stream->close();

        self::assertTrue($fs->status(path('aws-s3://file.txt'))->isFile());
        self::assertFalse($fs->status(path('aws-s3://file.txt'))->isDirectory());
        self::assertSame('Hello, World!', $fs->readFrom(path('aws-s3://file.txt'))->content());

        $fs->rm(path('aws-s3://file.txt'));
    }

    public function test_writing_to_to_aws_s3_from_resources() : void
    {
        $fs = aws_s3_filesystem($this->bucket(), $this->s3Client());

        $stream = $fs->writeTo(path('aws-s3://orders.csv'));
        $stream->fromResource(\fopen(__DIR__ . '/Fixtures/orders.csv', 'rb'));
        $stream->close();

        self::assertTrue($fs->status(path('aws-s3://orders.csv'))->isFile());
        self::assertFalse($fs->status(path('aws-s3://orders.csv'))->isDirectory());
        self::assertSame(\file_get_contents(__DIR__ . '/Fixtures/orders.csv'), $fs->readFrom(path('aws-s3://orders.csv'))->content());

        $fs->rm(path('aws-s3://orders.csv'));
    }

    public function test_writing_to_to_s3_using_blocks() : void
    {
        $fs = aws_s3_filesystem($this->bucket(), $this->s3Client());

        $stream = $fs->writeTo(path('aws-s3://block_blob.csv'));

        $string5Mb = \str_repeat('a', 1024 * 1024 * 5);

        for ($i = 0; $i < 10; $i++) {
            $stream->append($string5Mb . "\n");
        }

        $stream->close();

        self::assertTrue($fs->status(path('aws-s3://block_blob.csv'))->isFile());
        self::assertFalse($fs->status(path('aws-s3://block_blob.csv'))->isDirectory());

        $fs->rm(path('aws-s3://block_blob.csv'));
    }
}
