<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\SFTP\Tests\Integration;

use Flow\Filesystem\Path\Filter\OnlyFiles;

use function Flow\Filesystem\Bridge\SFTP\DSL\sftp_filesystem;
use function Flow\Filesystem\DSL\path;

final class DirectoryTraversalTest extends SFTPTestCase
{
    public function test_a_pattern_bound_to_one_year_does_not_list_the_other_years(): void
    {
        $this->sftpContext()->givenFileExists(path('sftp:///upload/2023/12/old.csv'), 'old');
        $this->sftpContext()->givenFileExists(path('sftp:///upload/2024/01/a.csv'), 'a');
        $this->sftpContext()->givenFileExists(path('sftp:///upload/2024/02/b.csv'), 'b');

        $sftp = $this->sftpContext()->recordingClient();
        $paths = [];

        foreach (sftp_filesystem($sftp)->list(path('sftp:///upload/2024/*/*.csv')) as $fileStatus) {
            $paths[] = $fileStatus->path->path();
        }

        static::assertSame(['/upload/2024/01/a.csv', '/upload/2024/02/b.csv'], $paths);
        static::assertSame(['/upload/2024', '/upload/2024/01', '/upload/2024/02'], $sftp->listedDirectories);
    }

    public function test_a_recursive_pattern_visits_every_directory_below_the_static_part(): void
    {
        $this->sftpContext()->givenFileExists(path('sftp:///upload/2024/01/a.csv'), 'a');
        $this->sftpContext()->givenFileExists(path('sftp:///upload/2024/01/deep/b.csv'), 'b');
        $this->sftpContext()->givenFileExists(path('sftp:///upload/2024/01/deep/c.json'), '[]');

        $sftp = $this->sftpContext()->recordingClient();
        $paths = [];

        foreach (sftp_filesystem($sftp)->list(path('sftp:///upload/**/*.csv')) as $fileStatus) {
            $paths[] = $fileStatus->path->path();
        }

        static::assertSame(['/upload/2024/01/a.csv', '/upload/2024/01/deep/b.csv'], $paths);
        static::assertSame(
            ['/upload', '/upload/2024', '/upload/2024/01', '/upload/2024/01/deep'],
            $sftp->listedDirectories,
        );
    }

    public function test_directories_are_reported_when_the_pattern_matches_them(): void
    {
        $this->sftpContext()->givenFileExists(path('sftp:///upload/2024/01/a.csv'), 'a');
        $this->sftpContext()->givenFileExists(path('sftp:///upload/2024/02/b.csv'), 'b');

        $paths = [];

        foreach (sftp_filesystem($this->sftpContext()->recordingClient())
            ->list(path('sftp:///upload/2024/*')) as $fileStatus) {
            $paths[] = $fileStatus->path->path() . ($fileStatus->isDirectory() ? '/' : '');
        }

        static::assertSame(['/upload/2024/01/', '/upload/2024/02/'], $paths);
    }

    public function test_entries_are_listed_in_a_stable_order(): void
    {
        $this->sftpContext()->givenFileExists(path('sftp:///upload/c.csv'), 'c');
        $this->sftpContext()->givenFileExists(path('sftp:///upload/a.csv'), 'a');
        $this->sftpContext()->givenFileExists(path('sftp:///upload/b.csv'), 'b');

        $paths = [];

        foreach (sftp_filesystem($this->sftpContext()->recordingClient())
            ->list(path('sftp:///upload'), new OnlyFiles()) as $status) {
            $paths[] = $status->path->path();
        }

        static::assertSame(['/upload/a.csv', '/upload/b.csv', '/upload/c.csv'], $paths);
    }
}
