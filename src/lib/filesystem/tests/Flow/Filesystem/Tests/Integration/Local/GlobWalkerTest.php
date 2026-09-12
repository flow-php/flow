<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Integration\Local;

use Flow\Filesystem\Local\GlobWalker;
use PHPUnit\Framework\TestCase;

use function Flow\Filesystem\DSL\native_local_filesystem;
use function Flow\Filesystem\DSL\path;
use function mkdir;
use function symlink;

final class GlobWalkerTest extends TestCase
{
    protected function setUp(): void
    {
        // an empty directory has no filesystem-library equivalent
        mkdir(__DIR__ . '/var/glob_walker/data/sub', recursive: true);
        native_local_filesystem()
            ->writeTo(path(__DIR__ . '/var/glob_walker/data/a.csv'))
            ->append('a')
            ->close();
        native_local_filesystem()
            ->writeTo(path(__DIR__ . '/var/glob_walker/real/sub/x.csv'))
            ->append('x')
            ->close();
    }

    protected function tearDown(): void
    {
        native_local_filesystem()->rm(path(__DIR__ . '/var/glob_walker'));
    }

    public function test_a_missing_base_directory_yields_nothing(): void
    {
        static::assertSame([], (new GlobWalker())->walk(path(__DIR__ . '/var/glob_walker/missing/*.csv')));
    }

    public function test_directories_matching_the_last_segment_are_returned(): void
    {
        static::assertSame(
            [__DIR__ . '/var/glob_walker/data/a.csv', __DIR__ . '/var/glob_walker/data/sub'],
            (new GlobWalker())->walk(path(__DIR__ . '/var/glob_walker/data/*')),
        );
    }

    public function test_double_star_does_not_descend_into_a_symlinked_directory(): void
    {
        symlink(__DIR__ . '/var/glob_walker/real', __DIR__ . '/var/glob_walker/data/linked');
        symlink(__DIR__ . '/var/glob_walker/data', __DIR__ . '/var/glob_walker/data/loop');

        static::assertSame([], (new GlobWalker())->walk(path(__DIR__ . '/var/glob_walker/data/**/x.csv')));
    }

    public function test_explicit_segments_follow_a_symlinked_directory(): void
    {
        symlink(__DIR__ . '/var/glob_walker/real', __DIR__ . '/var/glob_walker/data/linked');

        static::assertSame(
            [__DIR__ . '/var/glob_walker/data/linked/sub/x.csv'],
            (new GlobWalker())->walk(path(__DIR__ . '/var/glob_walker/data/*/sub/x.csv')),
        );
    }

    public function test_paths_are_sorted(): void
    {
        native_local_filesystem()
            ->writeTo(path(__DIR__ . '/var/glob_walker/sorted/0.csv'))
            ->append('0')
            ->close();
        native_local_filesystem()
            ->writeTo(path(__DIR__ . '/var/glob_walker/sorted/a/1.csv'))
            ->append('1')
            ->close();
        native_local_filesystem()
            ->writeTo(path(__DIR__ . '/var/glob_walker/sorted/b/1.csv'))
            ->append('1')
            ->close();

        static::assertSame(
            [
                __DIR__ . '/var/glob_walker/sorted/0.csv',
                __DIR__ . '/var/glob_walker/sorted/a/1.csv',
                __DIR__ . '/var/glob_walker/sorted/b/1.csv',
            ],
            (new GlobWalker())->walk(path(__DIR__ . '/var/glob_walker/sorted/**/*.csv')),
        );
    }

    public function test_consecutive_double_stars_match_like_one(): void
    {
        static::assertSame(
            [__DIR__ . '/var/glob_walker/real/sub/x.csv'],
            (new GlobWalker())->walk(path(__DIR__ . '/var/glob_walker/real/**/**/*.csv')),
        );
    }

    public function test_redundant_slashes_are_collapsed(): void
    {
        static::assertSame(
            [__DIR__ . '/var/glob_walker/real/sub/x.csv'],
            (new GlobWalker())->walk(path(__DIR__ . '/var/glob_walker/real/*//x.csv')),
        );
    }
}
