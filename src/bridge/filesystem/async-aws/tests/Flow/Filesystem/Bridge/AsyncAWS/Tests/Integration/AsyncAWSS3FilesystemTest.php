<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\AsyncAWS\Tests\Integration;

use Flow\Filesystem\FileStatus;
use Flow\Filesystem\Path\Filter\OnlyFiles;
use Flow\Filesystem\Tests\Context\GlobMatrixContext;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function array_map;
use function file_get_contents;
use function Flow\Filesystem\Bridge\AsyncAWS\DSL\aws_s3_filesystem;
use function Flow\Filesystem\DSL\path;
use function fopen;
use function iterator_to_array;
use function sort;
use function str_repeat;
use function strlen;
use function substr;

final class AsyncAWSS3FilesystemTest extends AsyncAWSS3TestCase
{
    public static function glob_matrix(): Generator
    {
        yield from GlobMatrixContext::patterns();
    }

    /**
     * @param list<string> $expected
     */
    #[DataProvider('glob_matrix')]
    public function test_list_agrees_with_the_shared_glob_matrix(string $pattern, array $expected): void
    {
        $fs = aws_s3_filesystem($this->bucket(), $this->s3Client());

        foreach (GlobMatrixContext::files() as $file) {
            $fs
                ->writeTo(path('aws-s3://' . $file))
                ->append($file)
                ->close();
        }

        $listed = array_map(
            static fn(FileStatus $status): string => substr($status->path->uri(), strlen('aws-s3://')),
            iterator_to_array($fs->list(path('aws-s3://' . $pattern), new OnlyFiles()), false),
        );
        sort($listed);

        static::assertSame($expected, $listed);
    }

    public function test_appending_to_existing_5mb_blob(): void
    {
        $fs = aws_s3_filesystem($this->bucket(), $this->s3Client());

        $fs
            ->writeTo(path('aws-s3://var/file.txt'))
            ->append(str_repeat('a', 1024 * 1024 * 5))
            ->close();

        $fs->appendTo(path('aws-s3://var/file.txt'))->append("This is second line\n")->close();

        $status = $fs->status(path('aws-s3://var/file.txt'));
        static::assertNotNull($status);
        static::assertTrue($status->isFile());
        static::assertFalse($status->isDirectory());

        static::assertSame(str_repeat('a', 1024), $fs->readFrom(path('aws-s3://var/file.txt'))->read(1024, 0));
        static::assertStringEndsWith("This is second line\n", $fs->readFrom(path('aws-s3://var/file.txt'))->read(
            58,
            1024 * 1024 * 5,
        ));

        $fs->rm(path('aws-s3://var/file.txt'));
    }

    public function test_appending_to_existing_blob(): void
    {
        $fs = aws_s3_filesystem($this->bucket(), $this->s3Client());

        $fs->writeTo(path('aws-s3://var/file.txt'))->append("This is first line\n")->close();

        $fs->appendTo(path('aws-s3://var/file.txt'))->append("This is second line\n")->close();

        $status = $fs->status(path('aws-s3://var/file.txt'));
        static::assertNotNull($status);
        static::assertTrue($status->isFile());
        static::assertFalse($status->isDirectory());
        static::assertSame(<<<'TXT'
            This is first line
            This is second line

            TXT, $fs->readFrom(path('aws-s3://var/file.txt'))->content());

        $fs->rm(path('aws-s3://var/file.txt'));
    }

    public function test_file_status_on_existing_file(): void
    {
        $fs = aws_s3_filesystem($this->bucket(), $this->s3Client());

        $resource = fopen(__DIR__ . '/Fixtures/orders.csv', 'rb');
        static::assertNotFalse($resource);
        $stream = $fs->writeTo(path('aws-s3://orders.csv'))->fromResource($resource);
        $stream->close();

        static::assertTrue($fs->status(path('aws-s3://orders.csv'))?->isFile());
    }

    public function test_file_status_on_existing_folder(): void
    {
        $fs = aws_s3_filesystem($this->bucket(), $this->s3Client());

        $resource = fopen(__DIR__ . '/Fixtures/orders.csv', 'rb');
        static::assertNotFalse($resource);
        $fs->writeTo(path('aws-s3://var/nested/orders.csv'))->fromResource($resource)->close();

        static::assertTrue($fs->status(path('aws-s3://var/nested'))?->isDirectory());
        static::assertTrue($fs->status(path('aws-s3://var/nested/'))?->isDirectory());
    }

    public function test_file_status_on_non_existing_file(): void
    {
        $fs = aws_s3_filesystem($this->bucket(), $this->s3Client());

        static::assertNull($fs->status(path('aws-s3://var/nested/orders.csv')));
    }

    public function test_file_status_on_non_existing_folder(): void
    {
        $fs = aws_s3_filesystem($this->bucket(), $this->s3Client());

        static::assertNull($fs->status(path('aws-s3://var/non-existing-folder/')));
    }

    public function test_file_status_on_non_existing_pattern(): void
    {
        $fs = aws_s3_filesystem($this->bucket(), $this->s3Client());

        static::assertNull($fs->status(path('aws-s3://var/non-existing-folder/*')));
    }

    public function test_file_status_on_partial_path(): void
    {
        $fs = aws_s3_filesystem($this->bucket(), $this->s3Client());

        $resource = fopen(__DIR__ . '/Fixtures/orders.csv', 'rb');
        static::assertNotFalse($resource);
        $stream = $fs->writeTo(path('aws-s3://var/some_path_to/file.txt'))->fromResource($resource);
        $stream->close();

        static::assertNull($fs->status(path('aws-s3://var/some_path')));
    }

    public function test_file_status_on_pattern(): void
    {
        $fs = aws_s3_filesystem($this->bucket(), $this->s3Client());

        $resource = fopen(__DIR__ . '/Fixtures/orders.csv', 'rb');
        static::assertNotFalse($resource);
        $stream = $fs->writeTo(path('aws-s3://var/some_path_to/file.txt'))->fromResource($resource);
        $stream->close();

        $status = $fs->status(path('aws-s3://var/some_path_to/*.txt'));
        static::assertNotNull($status);
        static::assertTrue($status->isFile());
        static::assertSame('aws-s3://var/some_path_to/file.txt', $status->path->uri());
    }

    public function test_file_status_on_root_folder(): void
    {
        $fs = aws_s3_filesystem($this->bucket(), $this->s3Client());

        static::assertTrue($fs->status(path('aws-s3:///'))?->isDirectory());
    }

    public function test_move_blob(): void
    {
        $fs = aws_s3_filesystem($this->bucket(), $this->s3Client());

        $fs->writeTo(path('aws-s3://var/file.txt'))->append('Hello, World!')->close();

        $fs->mv(path('aws-s3://var/file.txt'), path('aws-s3://var/file_mv.txt'));

        static::assertNull($fs->status(path('aws-s3://var/file.txt')));
        static::assertSame('Hello, World!', $fs->readFrom(path('aws-s3://var/file_mv.txt'))->content());
    }

    public function test_not_removing_a_content_when_its_not_a_full_folder_path_pattern(): void
    {
        $fs = aws_s3_filesystem($this->bucket(), $this->s3Client());

        $resource1 = fopen(__DIR__ . '/Fixtures/orders.csv', 'rb');
        static::assertNotFalse($resource1);
        $fs->writeTo(path('aws-s3://var/nested/orders/orders.txt'))->fromResource($resource1)->close();

        $resource2 = fopen(__DIR__ . '/Fixtures/orders.csv', 'rb');
        static::assertNotFalse($resource2);
        $fs->writeTo(path('aws-s3://var/nested/orders/orders.csv'))->fromResource($resource2)->close();

        $resource3 = fopen(__DIR__ . '/Fixtures/orders.csv', 'rb');
        static::assertNotFalse($resource3);
        $fs->writeTo(path('aws-s3://var/nested/orders/orders_01.csv'))->fromResource($resource3)->close();

        static::assertTrue($fs->status(path('aws-s3://var/nested/orders/orders.csv'))?->isFile());
        static::assertTrue($fs->status(path('aws-s3://var/nested/orders/orders_01.csv'))?->isFile());

        static::assertFalse($fs->rm(path('aws-s3://var/nested/orders/ord')));
    }

    public function test_remove_file_when_exists(): void
    {
        $fs = aws_s3_filesystem($this->bucket(), $this->s3Client());

        $stream = $fs->writeTo(path('aws-s3://var/flow-fs-test/remove_file_when_exists.txt'));
        $stream->append('some data to make file not empty');
        $stream->close();

        static::assertTrue($fs->status($stream->path())?->isFile());

        static::assertTrue($fs->rm($stream->path()));
        static::assertNull($fs->status($stream->path()));
    }

    public function test_remove_pattern(): void
    {
        $fs = aws_s3_filesystem($this->bucket(), $this->s3Client());

        $fs
            ->writeTo(path('aws-s3://var/flow-fs-test-directory/remove_file_when_exists.txt'))
            ->append('some data to make file not empty')
            ->close();
        $fs
            ->writeTo(path('aws-s3://var/flow-fs-test-directory/remove_file_when_exists.md'))
            ->append('some data to make file not empty')
            ->close();

        static::assertTrue($fs->status(path('aws-s3://var/flow-fs-test-directory/'))?->isDirectory());
        static::assertTrue(
            $fs->status(path('aws-s3://var/flow-fs-test-directory/remove_file_when_exists.txt'))?->isFile(),
        );
        $fs->rm(path('aws-s3://var/flow-fs-test-directory/*.txt'));
        static::assertTrue($fs->status(path('aws-s3://var/flow-fs-test-directory/'))?->isDirectory());
        static::assertNull($fs->status(path('aws-s3://var/flow-fs-test-directory/remove_file_when_exists.txt')));
        $fs->rm(path('aws-s3://var/flow-fs-test-directory/'));
    }

    public function test_removing_folder(): void
    {
        $fs = aws_s3_filesystem($this->bucket(), $this->s3Client());

        $resource1 = fopen(__DIR__ . '/Fixtures/orders.csv', 'rb');
        static::assertNotFalse($resource1);
        $fs->writeTo(path('aws-s3://var/orders.csv'))->fromResource($resource1)->close();

        $resource2 = fopen(__DIR__ . '/Fixtures/orders.csv', 'rb');
        static::assertNotFalse($resource2);
        $fs->writeTo(path('aws-s3://var/nested/orders/orders.csv'))->fromResource($resource2)->close();

        $resource3 = fopen(__DIR__ . '/Fixtures/orders.csv', 'rb');
        static::assertNotFalse($resource3);
        $fs->writeTo(path('aws-s3://var/nested/orders/orders_01.csv'))->fromResource($resource3)->close();

        static::assertTrue($fs->status(path('aws-s3://var/nested/orders/orders.csv'))?->isFile());
        static::assertTrue($fs->status(path('aws-s3://var/nested/orders/orders_01.csv'))?->isFile());

        $fs->rm(path('aws-s3://var/nested/orders'));

        static::assertTrue($fs->status(path('aws-s3://var/orders.csv'))?->isFile());
        static::assertNull($fs->status(path('aws-s3://var/nested/orders/orders.csv')));
        static::assertNull($fs->status(path('aws-s3://var/nested/orders/orders_01.csv')));
    }

    public function test_removing_folder_pattern(): void
    {
        $fs = aws_s3_filesystem($this->bucket(), $this->s3Client());

        $resource1 = fopen(__DIR__ . '/Fixtures/orders.csv', 'rb');
        static::assertNotFalse($resource1);
        $fs->writeTo(path('aws-s3://var/nested/orders/orders.txt'))->fromResource($resource1)->close();

        $resource2 = fopen(__DIR__ . '/Fixtures/orders.csv', 'rb');
        static::assertNotFalse($resource2);
        $fs->writeTo(path('aws-s3://var/nested/orders/orders.csv'))->fromResource($resource2)->close();

        $resource3 = fopen(__DIR__ . '/Fixtures/orders.csv', 'rb');
        static::assertNotFalse($resource3);
        $fs->writeTo(path('aws-s3://var/nested/orders/orders_01.csv'))->fromResource($resource3)->close();

        static::assertTrue($fs->status(path('aws-s3://var/nested/orders/orders.csv'))?->isFile());
        static::assertTrue($fs->status(path('aws-s3://var/nested/orders/orders_01.csv'))?->isFile());

        $fs->rm(path('aws-s3://var/nested/orders/*.csv'));

        static::assertTrue($fs->status(path('aws-s3://var/nested/orders/orders.txt'))?->isFile());
        static::assertNull($fs->status(path('aws-s3://var/nested/orders/orders.csv')));
        static::assertNull($fs->status(path('aws-s3://var/nested/orders/orders_01.csv')));
    }

    public function test_that_scan_sort_files_by_path_names(): void
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

        $paths = iterator_to_array($fs->list(path('aws-s3://multi_partitions/**/*.txt')));

        static::assertTrue($fs->status(path('aws-s3://multi_partitions/**/*.txt'))?->isFile());

        $uris = array_map(static fn(FileStatus $s): string => $s->path->uri(), $paths);
        static::assertSame(
            [
                path('aws-s3://multi_partitions/date=2022-01-01/country=de/file.txt')->uri(),
                path('aws-s3://multi_partitions/date=2022-01-01/country=pl/file.txt')->uri(),
                path('aws-s3://multi_partitions/date=2022-01-02/country=de/file.txt')->uri(),
                path('aws-s3://multi_partitions/date=2022-01-02/country=pl/file.txt')->uri(),
                path('aws-s3://multi_partitions/date=2022-01-03/country=de/file.txt')->uri(),
                path('aws-s3://multi_partitions/date=2022-01-03/country=pl/file.txt')->uri(),
                path('aws-s3://multi_partitions/date=2022-01-04/country=de/file.txt')->uri(),
                path('aws-s3://multi_partitions/date=2022-01-04/country=pl/file.txt')->uri(),
                path('aws-s3://multi_partitions/date=2022-01-05/country=de/file.txt')->uri(),
                path('aws-s3://multi_partitions/date=2022-01-05/country=pl/file.txt')->uri(),
            ],
            $uris,
        );
    }

    public function test_tmp_dir(): void
    {
        $fs = aws_s3_filesystem($this->bucket(), $this->s3Client());

        static::assertSame('aws-s3://_$azure_flow_tmp$/', $fs->getSystemTmpDir()->uri());
    }

    public function test_tmp_dir_status(): void
    {
        $fs = aws_s3_filesystem($this->bucket(), $this->s3Client());

        static::assertTrue($fs->status($fs->getSystemTmpDir())?->isDirectory());
    }

    public function test_write_to_tmp_dir(): void
    {
        $fs = aws_s3_filesystem($this->bucket(), $this->s3Client());

        $stream = $fs->writeTo($filePath = $fs->getSystemTmpDir()->suffix('file.txt'));
        $stream->append('Hello, World!');
        $stream->close();

        static::assertTrue($fs->status($filePath)?->isFile());
        static::assertSame('Hello, World!', $fs->readFrom($filePath)->content());

        $fs->rm($filePath);
    }

    public function test_write_to_tmp_dir_as_to_a_file(): void
    {
        $fs = aws_s3_filesystem($this->bucket(), $this->s3Client());

        $this->expectExceptionMessage('Cannot write to system tmp directory');

        $fs->writeTo($fs->getSystemTmpDir());
    }

    public function test_writing_to_aws_s3_storage(): void
    {
        $fs = aws_s3_filesystem($this->bucket(), $this->s3Client());

        $stream = $fs->writeTo(path('aws-s3://file.txt'));
        $stream->append('Hello, World!');
        $stream->close();

        $status = $fs->status(path('aws-s3://file.txt'));
        static::assertNotNull($status);
        static::assertTrue($status->isFile());
        static::assertFalse($status->isDirectory());
        static::assertSame('Hello, World!', $fs->readFrom(path('aws-s3://file.txt'))->content());

        $fs->rm(path('aws-s3://file.txt'));
    }

    public function test_writing_to_to_aws_s3_from_resources(): void
    {
        $fs = aws_s3_filesystem($this->bucket(), $this->s3Client());

        $stream = $fs->writeTo(path('aws-s3://orders.csv'));
        $resource = fopen(__DIR__ . '/Fixtures/orders.csv', 'rb');
        static::assertNotFalse($resource);
        $stream->fromResource($resource);
        $stream->close();

        $status = $fs->status(path('aws-s3://orders.csv'));
        static::assertNotNull($status);
        static::assertTrue($status->isFile());
        static::assertFalse($status->isDirectory());
        static::assertSame(
            file_get_contents(__DIR__ . '/Fixtures/orders.csv'),
            $fs->readFrom(path('aws-s3://orders.csv'))->content(),
        );

        $fs->rm(path('aws-s3://orders.csv'));
    }

    public function test_writing_to_to_s3_using_blocks(): void
    {
        $fs = aws_s3_filesystem($this->bucket(), $this->s3Client());

        $stream = $fs->writeTo(path('aws-s3://block_blob.csv'));

        $string5Mb = str_repeat('a', 1024 * 1024 * 5);

        for ($i = 0; $i < 10; $i++) {
            $stream->append($string5Mb . "\n");
        }

        $stream->close();

        $status = $fs->status(path('aws-s3://block_blob.csv'));
        static::assertNotNull($status);
        static::assertTrue($status->isFile());
        static::assertFalse($status->isDirectory());

        $fs->rm(path('aws-s3://block_blob.csv'));
    }
}
