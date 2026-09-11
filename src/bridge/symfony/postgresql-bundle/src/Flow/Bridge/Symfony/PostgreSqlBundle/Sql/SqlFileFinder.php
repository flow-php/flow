<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Sql;

use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Path;

use function Flow\Filesystem\DSL\path;
use function rtrim;

final readonly class SqlFileFinder
{
    public function __construct(
        private NativeLocalFilesystem $filesystem,
    ) {}

    /**
     * Resolves a Path (file, directory, or glob) into a list of `.sql` files.
     * Directories are scanned recursively.
     *
     * @return list<Path>
     */
    public function find(Path $path): array
    {
        $status = $this->filesystem->status($path);

        if ($status !== null && $status->isDirectory()) {
            $path = path(rtrim($path->path(), '/') . '/**/*.sql', $path->options());
        }

        $files = [];

        foreach ($this->filesystem->list($path) as $fileStatus) {
            if (!$fileStatus->isFile()) {
                continue;
            }

            if ($fileStatus->path->extension() !== 'sql') {
                continue;
            }

            $files[] = $fileStatus->path;
        }

        return $files;
    }
}
