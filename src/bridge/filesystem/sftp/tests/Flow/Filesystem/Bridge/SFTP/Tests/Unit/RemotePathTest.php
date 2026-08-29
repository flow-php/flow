<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\SFTP\Tests\Unit;

use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Bridge\SFTP\RemotePath;

use function Flow\Filesystem\DSL\path;

final class RemotePathTest extends FlowTestCase
{
    public function test_directory_of_a_file_in_the_root_is_the_root(): void
    {
        static::assertTrue(RemotePath::from(path('sftp:///orders.csv'))->directory()->isRoot());
    }

    public function test_root_is_recognized(): void
    {
        static::assertTrue(RemotePath::from(path('sftp:///'))->isRoot());
        static::assertFalse(RemotePath::from(path('sftp:///upload'))->isRoot());
    }

    public function test_trailing_slash_is_dropped_for_directory_listings(): void
    {
        static::assertSame('/upload', RemotePath::from(path('sftp:///upload/'))->withoutTrailingSlash());
    }

    public function test_trailing_slash_of_the_root_is_preserved_as_a_single_slash(): void
    {
        static::assertSame('/', RemotePath::from(path('sftp:///'))->withoutTrailingSlash());
    }
}
