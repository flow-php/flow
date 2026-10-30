<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Context;

use Flow\ETL\IO\Filesystem\SaveMode;
use Flow\ETL\IO\Loader\File\FileCommit;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Path;
use RuntimeException;

use function file_put_contents;
use function is_string;
use function mkdir;

final readonly class FileCommitContext
{
    public function __construct(
        private Path $root,
        private Filesystem $filesystem,
    ) {}

    public function commit(Path $destination, SaveMode $mode, ?Filesystem $filesystem = null): FileCommit
    {
        return new FileCommit($filesystem ?? $this->filesystem, $destination, $mode, 'r1');
    }

    /**
     * Creates $tree under the root: a string value is a file's content, an array value a directory.
     *
     * @param array<string, array<string, mixed>|string> $tree
     */
    public function files(array $tree, string $under = ''): void
    {
        foreach ($tree as $name => $content) {
            $target = $this->root->path() . $under . '/' . $name;

            if (is_string($content)) {
                file_put_contents($target, $content) !== false
                    || throw new RuntimeException('Could not create ' . $target);

                continue;
            }

            mkdir($target, recursive: true);

            /** @var array<string, array<string, mixed>|string> $content */
            $this->files($content, $under . '/' . $name);
        }
    }

    public function path(string $relative): Path
    {
        return $this->root->suffix('/' . $relative);
    }
}
