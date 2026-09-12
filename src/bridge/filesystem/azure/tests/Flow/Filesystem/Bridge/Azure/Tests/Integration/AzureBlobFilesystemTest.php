<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\Azure\Tests\Integration;

use Flow\Filesystem\Bridge\Azure\Options;
use Flow\Filesystem\FileStatus;
use Flow\Filesystem\Path\Filter\OnlyFiles;
use Flow\Filesystem\Tests\Context\GlobMatrixContext;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function array_map;
use function file_get_contents;
use function Flow\Filesystem\Bridge\Azure\DSL\azure_filesystem;
use function Flow\Filesystem\Bridge\Azure\DSL\azure_filesystem_options;
use function Flow\Filesystem\DSL\path;
use function fopen;
use function iterator_to_array;
use function sort;
use function str_repeat;
use function strlen;
use function substr;

final class AzureBlobFilesystemTest extends AzureBlobServiceTestCase
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
        $fs = azure_filesystem($this->blobService('flow-php'));

        foreach (GlobMatrixContext::files() as $file) {
            $fs
                ->writeTo(path('azure-blob://' . $file))
                ->append($file)
                ->close();
        }

        $listed = array_map(
            static fn(FileStatus $status): string => substr($status->path->uri(), strlen('azure-blob://')),
            iterator_to_array($fs->list(path('azure-blob://' . $pattern), new OnlyFiles()), false),
        );
        sort($listed);

        static::assertSame($expected, $listed);
    }

    public function test_appending_to_existing_blob(): void
    {
        $fs = azure_filesystem($this->blobService('flow-php'));

        $stream = $fs->writeTo(path('azure-blob://file.txt'));
        $stream->append("This is first line\n");
        $stream->close();

        $stream = $fs->appendTo(path('azure-blob://file.txt'));
        $stream->append("This is second line\n");
        $stream->close();

        $status = $fs->status(path('azure-blob://file.txt'));
        static::assertNotNull($status);
        static::assertTrue($status->isFile());
        static::assertFalse($status->isDirectory());
        static::assertSame(<<<'TXT'
            This is first line
            This is second line

            TXT, $fs->readFrom(path('azure-blob://file.txt'))->content());

        $fs->rm(path('azure-blob://file.txt'));
    }

    public function test_appending_to_existing_block_blob_new_blocks(): void
    {
        $fs = azure_filesystem($this->blobService('flow-php'), (new Options())->withBlockSize(1024));

        $stream = $fs->writeTo(path('azure-blob://file.txt'));
        $output = '';

        for ($i = 0; $i < 10; $i++) {
            $output .= str_repeat('a', 1024) . "\n";
            $stream->append(str_repeat('a', 1024) . "\n");
        }
        $stream->close();

        $stream = $fs->appendTo(path('azure-blob://file.txt'));

        for ($i = 0; $i < 10; $i++) {
            $output .= str_repeat('n', 1024) . "\n";
            $stream->append(str_repeat('n', 1024) . "\n");
        }
        $stream->close();

        $status = $fs->status(path('azure-blob://file.txt'));
        static::assertNotNull($status);
        static::assertTrue($status->isFile());
        static::assertFalse($status->isDirectory());
        static::assertSame($output, $fs->readFrom(path('azure-blob://file.txt'))->content());

        $fs->rm(path('azure-blob://file.txt'));
    }

    public function test_appending_to_existing_non_block_blob_new_blocks(): void
    {
        $fs = azure_filesystem($this->blobService('flow-php'), (new Options())->withBlockSize(1024));

        $stream = $fs->writeTo(path('azure-blob://file.txt'));
        $stream->append("This is first line\n");
        $stream->close();

        $stream = $fs->appendTo(path('azure-blob://file.txt'));
        $output = "This is first line\n";

        for ($i = 0; $i < 10; $i++) {
            $output .= str_repeat('a', 1024) . "\n";
            $stream->append(str_repeat('a', 1024) . "\n");
        }
        $stream->close();

        $status = $fs->status(path('azure-blob://file.txt'));
        static::assertNotNull($status);
        static::assertTrue($status->isFile());
        static::assertFalse($status->isDirectory());
        static::assertSame($output, $fs->readFrom(path('azure-blob://file.txt'))->content());

        $fs->rm(path('azure-blob://file.txt'));
    }

    public function test_file_status_on_existing_file(): void
    {
        $fs = azure_filesystem($this->blobService('flow-php'));

        $resource = fopen(__DIR__ . '/Fixtures/orders.csv', 'rb');
        static::assertIsResource($resource);
        $fs->writeTo(path('azure-blob://file.txt'))->fromResource($resource)->close();

        static::assertTrue($fs->status(path('azure-blob://file.txt'))?->isFile());
    }

    public function test_file_status_on_existing_folder(): void
    {
        $fs = azure_filesystem($this->blobService('flow-php'));

        $resource = fopen(__DIR__ . '/Fixtures/orders.csv', 'rb');
        static::assertIsResource($resource);
        $fs->writeTo(path('azure-blob://nested/orders/orders.txt'))->fromResource($resource)->close();

        static::assertTrue($fs->status(path('azure-blob://nested/orders'))?->isDirectory());
        static::assertTrue($fs->status(path('azure-blob://nested/orders/'))?->isDirectory());
    }

    public function test_file_status_on_non_existing_file(): void
    {
        $fs = azure_filesystem($this->blobService('flow-php'));

        static::assertNull($fs->status(path('azure-blob://non-existing-file.txt')));
    }

    public function test_file_status_on_non_existing_folder(): void
    {
        $fs = azure_filesystem($this->blobService('flow-php'));

        static::assertNull($fs->status(path('azure-blob://non-existing-folder/')));
    }

    public function test_file_status_on_non_existing_pattern(): void
    {
        $fs = azure_filesystem($this->blobService('flow-php'));

        static::assertNull($fs->status(path('azure-blob://non-existing-folder/*')));
    }

    public function test_file_status_on_partial_path(): void
    {
        $fs = azure_filesystem($this->blobService('flow-php'));

        $resource = fopen(__DIR__ . '/Fixtures/orders.csv', 'rb');
        static::assertIsResource($resource);
        $fs->writeTo(path('azure-blob://some_path_to/file.txt'))->fromResource($resource)->close();

        static::assertNull($fs->status(path('azure-blob://some_path')));
    }

    public function test_file_status_on_pattern(): void
    {
        $fs = azure_filesystem($this->blobService('flow-php'));

        $resource = fopen(__DIR__ . '/Fixtures/orders.csv', 'rb');
        static::assertIsResource($resource);
        $fs->writeTo(path('azure-blob://some_path_to/file.txt'))->fromResource($resource)->close();

        $status = $fs->status(path('azure-blob://some_path_to/*.txt'));
        static::assertNotNull($status);
        static::assertTrue($status->isFile());
        static::assertSame('azure-blob://some_path_to/file.txt', $status->path->uri());
    }

    public function test_file_status_on_root_folder(): void
    {
        $fs = azure_filesystem($this->blobService('flow-php'));

        static::assertTrue($fs->status(path('azure-blob://'))?->isDirectory());
    }

    public function test_move_blob(): void
    {
        $fs = azure_filesystem($this->blobService('flow-php'));

        $fs->writeTo(path('azure-blob://file.txt'))->append('Hello, World!')->close();

        $fs->mv(path('azure-blob://file.txt'), path('azure-blob://file_mv.txt'));

        static::assertNull($fs->status(path('azure-blob://file.txt')));
        static::assertSame('Hello, World!', $fs->readFrom(path('azure-blob://file_mv.txt'))->content());
    }

    public function test_not_removing_a_content_when_its_not_a_full_folder_path_pattern(): void
    {
        $fs = azure_filesystem($this->blobService('flow-php'));

        $resource1 = fopen(__DIR__ . '/Fixtures/orders.csv', 'rb');
        static::assertIsResource($resource1);
        $fs->writeTo(path('azure-blob://nested/orders/orders.txt'))->fromResource($resource1)->close();
        $resource2 = fopen(__DIR__ . '/Fixtures/orders.csv', 'rb');
        static::assertIsResource($resource2);
        $fs->writeTo(path('azure-blob://nested/orders/orders.csv'))->fromResource($resource2)->close();
        $resource3 = fopen(__DIR__ . '/Fixtures/orders.csv', 'rb');
        static::assertIsResource($resource3);
        $fs->writeTo(path('azure-blob://nested/orders/orders_01.csv'))->fromResource($resource3)->close();

        static::assertTrue($fs->status(path('azure-blob://nested/orders/orders.csv'))?->isFile());
        static::assertTrue($fs->status(path('azure-blob://nested/orders/orders_01.csv'))?->isFile());

        static::assertFalse($fs->rm(path('azure-blob://nested/orders/ord')));
    }

    public function test_removing_folder(): void
    {
        $fs = azure_filesystem($this->blobService('flow-php'));

        $resource1 = fopen(__DIR__ . '/Fixtures/orders.csv', 'rb');
        static::assertIsResource($resource1);
        $fs->writeTo(path('azure-blob://orders.csv'))->fromResource($resource1)->close();
        $resource2 = fopen(__DIR__ . '/Fixtures/orders.csv', 'rb');
        static::assertIsResource($resource2);
        $fs->writeTo(path('azure-blob://nested/orders/orders.csv'))->fromResource($resource2)->close();
        $resource3 = fopen(__DIR__ . '/Fixtures/orders.csv', 'rb');
        static::assertIsResource($resource3);
        $fs->writeTo(path('azure-blob://nested/orders/orders_01.csv'))->fromResource($resource3)->close();

        static::assertTrue($fs->status(path('azure-blob://nested/orders/orders.csv'))?->isFile());
        static::assertTrue($fs->status(path('azure-blob://nested/orders/orders_01.csv'))?->isFile());

        $fs->rm(path('azure-blob://nested/orders'));

        static::assertTrue($fs->status(path('azure-blob://orders.csv'))?->isFile());
        static::assertNull($fs->status(path('azure-blob://nested/orders/orders.csv')));
        static::assertNull($fs->status(path('azure-blob://nested/orders/orders_01.csv')));
    }

    public function test_removing_folder_pattern(): void
    {
        $fs = azure_filesystem($this->blobService('flow-php'));

        $resource1 = fopen(__DIR__ . '/Fixtures/orders.csv', 'rb');
        static::assertIsResource($resource1);
        $fs->writeTo(path('azure-blob://nested/orders/orders.txt'))->fromResource($resource1)->close();
        $resource2 = fopen(__DIR__ . '/Fixtures/orders.csv', 'rb');
        static::assertIsResource($resource2);
        $fs->writeTo(path('azure-blob://nested/orders/orders.csv'))->fromResource($resource2)->close();
        $resource3 = fopen(__DIR__ . '/Fixtures/orders.csv', 'rb');
        static::assertIsResource($resource3);
        $fs->writeTo(path('azure-blob://nested/orders/orders_01.csv'))->fromResource($resource3)->close();

        static::assertTrue($fs->status(path('azure-blob://nested/orders/orders.csv'))?->isFile());
        static::assertTrue($fs->status(path('azure-blob://nested/orders/orders_01.csv'))?->isFile());

        $fs->rm(path('azure-blob://nested/orders/*.csv'));

        static::assertTrue($fs->status(path('azure-blob://nested/orders/orders.txt'))?->isFile());
        static::assertNull($fs->status(path('azure-blob://nested/orders/orders.csv')));
        static::assertNull($fs->status(path('azure-blob://nested/orders/orders_01.csv')));
    }

    public function test_rm_tmp_dir(): void
    {
        $fs = azure_filesystem($this->blobService('flow-php'));

        static::assertFalse($fs->rm($fs->getSystemTmpDir()));
    }

    public function test_tmp_dir(): void
    {
        $fs = azure_filesystem($this->blobService('flow-php'));

        static::assertSame('azure-blob://_$azure_flow_tmp$/', $fs->getSystemTmpDir()->uri());
    }

    public function test_tmp_dir_status(): void
    {
        $fs = azure_filesystem($this->blobService('flow-php'));

        static::assertTrue($fs->status($fs->getSystemTmpDir())?->isDirectory());
    }

    public function test_write_to_custom_tmp_dir(): void
    {
        $fs = azure_filesystem(
            $this->blobService('flow-php'),
            azure_filesystem_options()->withTmpDir(path('azure-blob://custom-tmp-dir/')),
        );

        $stream = $fs->writeTo($fs->getSystemTmpDir()->suffix('file.txt'));
        $stream->append('Hello, World!');
        $stream->close();

        static::assertTrue($fs->status(path('azure-blob://custom-tmp-dir/file.txt'))?->isFile());
        static::assertSame('Hello, World!', $fs->readFrom(path('azure-blob://custom-tmp-dir/file.txt'))->content());

        $fs->rm($fs->getSystemTmpDir()->suffix('file.txt'));
    }

    public function test_write_to_tmp_dir(): void
    {
        $fs = azure_filesystem($this->blobService('flow-php'));

        $stream = $fs->writeTo($fs->getSystemTmpDir()->suffix('file.txt'));
        $stream->append('Hello, World!');
        $stream->close();

        static::assertTrue($fs->status($fs->getSystemTmpDir()->suffix('file.txt'))?->isFile());
        static::assertSame('Hello, World!', $fs->readFrom($fs->getSystemTmpDir()->suffix('file.txt'))->content());

        $fs->rm($fs->getSystemTmpDir()->suffix('file.txt'));
    }

    public function test_write_to_tmp_dir_as_to_a_file(): void
    {
        $fs = azure_filesystem($this->blobService('flow-php'));

        $this->expectExceptionMessage('Cannot write to system tmp directory');

        $fs->writeTo($fs->getSystemTmpDir());
    }

    public function test_writing_to_azure_blob_storage(): void
    {
        $fs = azure_filesystem($this->blobService('flow-php'));

        $stream = $fs->writeTo(path('azure-blob://file.txt'));
        $stream->append('Hello, World!');
        $stream->close();

        $status = $fs->status(path('azure-blob://file.txt'));
        static::assertNotNull($status);
        static::assertTrue($status->isFile());
        static::assertFalse($status->isDirectory());
        static::assertSame('Hello, World!', $fs->readFrom(path('azure-blob://file.txt'))->content());

        $fs->rm(path('azure-blob://file.txt'));
    }

    public function test_writing_to_to_azure_from_resources(): void
    {
        $fs = azure_filesystem($this->blobService('flow-php'));

        $stream = $fs->writeTo(path('azure-blob://orders.csv'));
        $resource = fopen(__DIR__ . '/Fixtures/orders.csv', 'rb');
        static::assertIsResource($resource);
        $stream->fromResource($resource);
        $stream->close();

        $status = $fs->status(path('azure-blob://orders.csv'));
        static::assertNotNull($status);
        static::assertTrue($status->isFile());
        static::assertFalse($status->isDirectory());
        static::assertSame(
            file_get_contents(__DIR__ . '/Fixtures/orders.csv'),
            $fs->readFrom(path('azure-blob://orders.csv'))->content(),
        );

        $fs->rm(path('azure-blob://orders.csv'));
    }

    public function test_writing_to_to_azure_using_blocks(): void
    {
        $fs = azure_filesystem($this->blobService('flow-php'), (new Options())->withBlockSize(1024));

        $stream = $fs->writeTo(path('azure-blob://block_blob.csv'));

        for ($i = 0; $i < 10; $i++) {
            $stream->append(str_repeat('a', 1024) . "\n");
        }

        $stream->close();

        $status = $fs->status(path('azure-blob://block_blob.csv'));
        static::assertNotNull($status);
        static::assertTrue($status->isFile());
        static::assertFalse($status->isDirectory());

        $fs->rm(path('azure-blob://block_blob.csv'));
    }
}
