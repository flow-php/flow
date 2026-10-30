<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\IO\Filesystem;

use Flow\ETL\Foundation\Exception\RuntimeException;
use Flow\ETL\IO\Loader\File\FileCommit;
use Flow\ETL\Tests\Context\WrittenFiles;
use Flow\ETL\Tests\FlowIntegrationTestCase;
use Flow\Filesystem\FileStatus;

use function array_map;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\overwrite;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;
use function Flow\Floe\DSL\from_floe;
use function Flow\Floe\DSL\to_floe;
use function iterator_to_array;

/**
 * The write-side properties of the commit protocol: Overwrite's tmp->final rename with its stale sweep, and a save mode
 * per loader. The per-format properties (a header written once, a close tag once per file) run in their adapter.
 */
final class FileWritePropertiesTest extends FlowIntegrationTestCase
{
    public function test_overwrite_promotes_the_temporary_output(): void
    {
        $destination = $this->cacheDir->suffix('overwrite.floe');

        df()
            ->read(from_array([['id' => 1]]))
            ->write(to_floe($destination))
            ->run();
        df()
            ->read(from_array([['id' => 2]]))
            ->write(to_floe($destination)->saveMode(overwrite()))
            ->run();

        static::assertSame([['id' => 2]], df()->read(from_floe($destination))->fetch()->toArray());
        static::assertSame([], WrittenFiles::temporaryOutputs($this->cacheDir));
    }

    public function test_overwrite_on_a_memory_filesystem_replaces_the_file(): void
    {
        $filesystem = memory_filesystem();
        df()
            ->read(from_array([['id' => 1]]))
            ->write(to_floe(path('memory://out/x.floe'), filesystem: $filesystem))
            ->run();

        df()
            ->read(from_array([['id' => 2]]))
            ->write(to_floe(path('memory://out/x.floe'), filesystem: $filesystem)->saveMode(overwrite()))
            ->run();

        static::assertSame(
            [['id' => 2]],
            df()
                ->read(from_floe(path('memory://out/x.floe'), filesystem: $filesystem))
                ->fetch()
                ->toArray(),
        );
        static::assertSame(
            ['memory://out/x.floe'],
            array_map(
                static fn(FileStatus $status): string => $status->path->uri(),
                iterator_to_array($filesystem->list(path('memory://out/**/*')), false),
            ),
        );
    }

    public function test_a_read_during_a_staged_overwrite_does_not_see_the_temporary_output(): void
    {
        df()
            ->read(from_array([['id' => 1]]))
            ->write(to_floe($this->cacheDir->suffix('/out/part-{part}.floe')))
            ->run();
        $commit = new FileCommit($this->fs(), $this->cacheDir->suffix('/out/part-{part}.floe'), overwrite(), 'r1');
        $commit->open()->append('staged, never read');

        static::assertCount(1, WrittenFiles::temporaryOutputs($this->cacheDir));
        static::assertSame(
            [['id' => 1]],
            df()
                ->read(from_floe($this->cacheDir->suffix('/out/**/*.floe')))
                ->fetch()
                ->toArray(),
        );

        $commit->abort();
    }

    public function test_save_mode_is_per_loader(): void
    {
        $overwritten = $this->cacheDir->suffix('per-loader-overwrite.floe');
        $strict = $this->cacheDir->suffix('per-loader-strict.floe');

        df()
            ->read(from_array([['id' => 1]]))
            ->write(to_floe($overwritten))
            ->run();
        df()
            ->read(from_array([['id' => 1]]))
            ->write(to_floe($strict))
            ->run();

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('already exists');

        // the first loader overwrites, the second must still refuse - the mode does not leak between them
        df()
            ->read(from_array([['id' => 2]]))
            ->write(to_floe($overwritten)->saveMode(overwrite()))
            ->write(to_floe($strict))
            ->run();
    }

    public function test_two_distinct_loaders_on_one_path_throw(): void
    {
        $destination = $this->cacheDir->suffix('two-loaders.floe');

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('already exists');

        df()
            ->read(from_array([['id' => 1]]))
            ->write(to_floe($destination))
            ->write(to_floe($destination))
            ->run();
    }
}
