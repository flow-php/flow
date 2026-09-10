<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Tests\Context;

use Flow\Benchmarks\Datasets\DigestCache;
use Flow\Benchmarks\Datasets\Paths;

use function dirname;
use function file_put_contents;
use function Flow\Filesystem\DSL\native_local_filesystem;
use function Flow\Filesystem\DSL\path;
use function is_dir;
use function mkdir;
use function uniqid;

/**
 * A throwaway tree of source files under benchmarks/var, addressable by the repo-root-relative
 * path SourceTreeDigest expects.
 */
final readonly class SourceTree
{
    private string $relativePath;

    public function __construct(string $name)
    {
        $this->relativePath = 'benchmarks/var/source_tree/' . $name . '_' . uniqid('', true);
    }

    public function absolutePath(): string
    {
        return Paths::projectRoot() . '/' . $this->relativePath;
    }

    public function relativePath(): string
    {
        return $this->relativePath;
    }

    public function remove(): void
    {
        DigestCache::clear();

        if (is_dir($this->absolutePath())) {
            native_local_filesystem()->rm(path($this->absolutePath()));
        }
    }

    public function write(string $file, string $contents): void
    {
        DigestCache::clear();

        $target = $this->absolutePath() . '/' . $file;

        if (!is_dir(dirname($target))) {
            mkdir(dirname($target), 0777, true);
        }

        file_put_contents($target, $contents);
    }
}
