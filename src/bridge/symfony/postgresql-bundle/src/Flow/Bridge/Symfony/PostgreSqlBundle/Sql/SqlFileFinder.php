<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Sql;

use function Flow\Filesystem\DSL\path;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Path;

final readonly class SqlFileFinder
{
    public function __construct(private NativeLocalFilesystem $filesystem)
    {
    }

    /**
     * Resolves a Path (file, directory, or glob) into a list of `.sql` files.
     * Directories are scanned recursively.
     *
     * @return list<Path>
     */
    public function find(Path $path) : array
    {
        $status = $this->filesystem->status($path);

        if ($status !== null && $status->isDirectory()) {
            $path = path(\rtrim($path->path(), '/') . '/**/*.sql', $path->options());
        }

        $files = [];

        foreach ($this->filesystem->list($path) as $file) {
            if (!$file->isFile()) {
                continue;
            }

            if ($file->path->extension() !== 'sql') {
                continue;
            }

            $files[] = $file->path;
        }

        return $files;
    }
}
