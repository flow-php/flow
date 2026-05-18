<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Integration\Sql;

use Flow\Bridge\Symfony\PostgreSqlBundle\Sql\SqlFileFinder;
use Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Context\FilesystemContext;
use PHPUnit\Framework\TestCase;

use function array_map;
use function basename;
use function Flow\Filesystem\DSL\native_local_filesystem;
use function sort;

final class SqlFileFinderTest extends TestCase
{
    private SqlFileFinder $finder;

    private FilesystemContext $fs;

    protected function setUp(): void
    {
        $this->fs = new FilesystemContext('flow_sql_finder_');
        $this->finder = new SqlFileFinder(native_local_filesystem());
    }

    protected function tearDown(): void
    {
        $this->fs->cleanup();
    }

    public function test_finds_single_sql_file_by_exact_path(): void
    {
        $file = $this->fs->writeFile('query.sql', 'SELECT 1');

        $found = $this->finder->find($file);

        static::assertCount(1, $found);
        static::assertSame($file->path(), $found[0]->path());
    }

    public function test_finds_sql_files_recursively_in_directory(): void
    {
        $this->fs->writeFile('a.sql', 'SELECT 1');
        $this->fs->writeFile('nested/deep/b.sql', 'SELECT 2');
        $this->fs->writeFile('nested/c.sql', 'SELECT 3');

        $paths = array_map(static fn($p) => basename($p->path()), $this->finder->find($this->fs->path()));

        sort($paths);
        static::assertSame(['a.sql', 'b.sql', 'c.sql'], $paths);
    }

    public function test_finds_sql_files_via_glob_pattern(): void
    {
        $this->fs->writeFile('one.sql', 'SELECT 1');
        $this->fs->writeFile('two.sql', 'SELECT 2');
        $this->fs->writeFile('skip.txt', 'nope');

        $found = $this->finder->find($this->fs->path('*.sql'));

        static::assertCount(2, $found);
    }

    public function test_returns_empty_list_for_empty_directory(): void
    {
        static::assertSame([], $this->finder->find($this->fs->path()));
    }

    public function test_skips_non_sql_extensions_in_directory(): void
    {
        $this->fs->writeFile('keep.sql', 'SELECT 1');
        $this->fs->writeFile('skip.txt', 'nope');
        $this->fs->writeFile('skip.md', 'nope');

        $found = $this->finder->find($this->fs->path());

        static::assertCount(1, $found);
        static::assertSame('keep.sql', basename($found[0]->path()));
    }

    public function test_skips_non_sql_files_matched_by_broad_glob(): void
    {
        $this->fs->writeFile('a.sql', 'SELECT 1');
        $this->fs->writeFile('b.txt', 'nope');

        $found = $this->finder->find($this->fs->path('*'));

        static::assertCount(1, $found);
        static::assertSame('a.sql', basename($found[0]->path()));
    }
}
