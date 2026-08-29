<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\SFTP\Tests\Integration;

use Flow\Filesystem\FileStatus;
use Flow\Filesystem\Path\Filter\OnlyFiles;

use function Flow\Filesystem\DSL\path;
use function iterator_to_array;
use function strlen;

final class SFTPFilesystemTest extends SFTPTestCase
{
    public function test_listing_a_directory_walks_it_recursively(): void
    {
        $this->sftpContext()->givenFileExists(path('sftp:///upload/2024/01/a.csv'), "id\n1\n");
        $this->sftpContext()->givenFileExists(path('sftp:///upload/2024/02/b.csv'), "id\n2\n");

        $paths = [];

        foreach ($this->sftpContext()->filesystem()->list(path('sftp:///upload'), new OnlyFiles()) as $fileStatus) {
            $paths[] = $fileStatus->path->path();
        }

        static::assertSame(['/upload/2024/01/a.csv', '/upload/2024/02/b.csv'], $paths);
    }

    public function test_listing_a_pattern_returns_only_matching_files(): void
    {
        $this->sftpContext()->givenFileExists(path('sftp:///upload/orders.csv'), "id\n1\n");
        $this->sftpContext()->givenFileExists(path('sftp:///upload/orders.json'), '[]');

        $paths = [];

        foreach ($this->sftpContext()->filesystem()->list(path('sftp:///upload/*.csv')) as $fileStatus) {
            $paths[] = $fileStatus->path->path();
        }

        static::assertSame(['/upload/orders.csv'], $paths);
    }

    public function test_listing_a_single_file_yields_it_with_size_and_modification_time(): void
    {
        $this->sftpContext()->givenFileExists(path('sftp:///upload/orders.csv'), $content = "id,name\n1,one\n");

        $statuses = iterator_to_array(
            $this->sftpContext()->filesystem()->list(path('sftp:///upload/orders.csv')),
            false,
        );

        static::assertCount(1, $statuses);
        static::assertInstanceOf(FileStatus::class, $statuses[0]);
        static::assertTrue($statuses[0]->isFile());
        static::assertSame(strlen($content), $statuses[0]->size);
        static::assertNotNull($statuses[0]->lastModifiedAt);
    }

    public function test_moving_a_file_creates_missing_parent_directories(): void
    {
        $this->sftpContext()->givenFileExists(path('sftp:///upload/orders.csv'), "id\n1\n");

        static::assertTrue($this->sftpContext()->filesystem()->mv(
            path('sftp:///upload/orders.csv'),
            path('sftp:///upload/archive/2024/orders.csv'),
        ));
        static::assertFalse($this->sftpContext()->exists(path('sftp:///upload/orders.csv')));
        static::assertSame("id\n1\n", $this->sftpContext()->contentOf(path('sftp:///upload/archive/2024/orders.csv')));
    }

    public function test_moving_a_missing_file_reports_failure(): void
    {
        static::assertFalse($this->sftpContext()->filesystem()->mv(
            path('sftp:///upload/missing.csv'),
            path('sftp:///upload/moved.csv'),
        ));
    }

    public function test_moving_over_an_existing_file_replaces_it(): void
    {
        $this->sftpContext()->givenFileExists(path('sftp:///upload/source.csv'), 'source');
        $this->sftpContext()->givenFileExists(path('sftp:///upload/target.csv'), 'target');

        static::assertTrue($this->sftpContext()->filesystem()->mv(
            path('sftp:///upload/source.csv'),
            path('sftp:///upload/target.csv'),
        ));
        static::assertSame('source', $this->sftpContext()->contentOf(path('sftp:///upload/target.csv')));
    }

    public function test_removing_a_directory_removes_everything_below_it(): void
    {
        $this->sftpContext()->givenFileExists(path('sftp:///upload/archive/2024/a.csv'), 'a');
        $this->sftpContext()->givenFileExists(path('sftp:///upload/archive/2024/b.csv'), 'b');

        static::assertTrue($this->sftpContext()->filesystem()->rm(path('sftp:///upload/archive')));
        static::assertFalse($this->sftpContext()->exists(path('sftp:///upload/archive')));
    }

    public function test_removing_a_missing_file_reports_failure(): void
    {
        static::assertFalse($this->sftpContext()->filesystem()->rm(path('sftp:///upload/missing.csv')));
    }

    public function test_removing_a_pattern_removes_only_matching_files(): void
    {
        $this->sftpContext()->givenFileExists(path('sftp:///upload/a.csv'), 'a');
        $this->sftpContext()->givenFileExists(path('sftp:///upload/b.csv'), 'b');
        $this->sftpContext()->givenFileExists(path('sftp:///upload/keep.json'), '[]');

        static::assertTrue($this->sftpContext()->filesystem()->rm(path('sftp:///upload/*.csv')));
        static::assertFalse($this->sftpContext()->exists(path('sftp:///upload/a.csv')));
        static::assertFalse($this->sftpContext()->exists(path('sftp:///upload/b.csv')));
        static::assertTrue($this->sftpContext()->exists(path('sftp:///upload/keep.json')));
    }

    public function test_status_of_a_directory(): void
    {
        $this->sftpContext()->givenFileExists(path('sftp:///upload/archive/orders.csv'), 'a');

        $status = $this->sftpContext()->filesystem()->status(path('sftp:///upload/archive'));

        static::assertNotNull($status);
        static::assertTrue($status->isDirectory());
    }

    public function test_status_of_a_file(): void
    {
        $this->sftpContext()->givenFileExists(path('sftp:///upload/orders.csv'), $content = "id,name\n1,one\n");

        $status = $this->sftpContext()->filesystem()->status(path('sftp:///upload/orders.csv'));

        static::assertNotNull($status);
        static::assertTrue($status->isFile());
        static::assertSame(strlen($content), $status->size);
    }

    public function test_status_of_a_missing_file_is_null(): void
    {
        static::assertNull($this->sftpContext()->filesystem()->status(path('sftp:///upload/missing.csv')));
    }

    public function test_status_of_a_pattern_returns_the_first_match(): void
    {
        $this->sftpContext()->givenFileExists(path('sftp:///upload/a.csv'), 'a');
        $this->sftpContext()->givenFileExists(path('sftp:///upload/b.csv'), 'b');

        static::assertSame(
            '/upload/a.csv',
            $this->sftpContext()->filesystem()->status(path('sftp:///upload/*.csv'))?->path->path(),
        );
    }
}
