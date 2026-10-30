<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Context;

use Flow\Filesystem\Path;
use RecursiveDirectoryIterator;
use RecursiveIteratorIterator;
use SplFileInfo;

use function Flow\Filesystem\DSL\native_local_filesystem;
use function is_dir;
use function sort;
use function str_starts_with;
use function strlen;
use function substr;

final class WrittenFiles
{
    /**
     * @return list<string> the files under $directory, relative, sorted
     */
    public static function under(Path $directory): array
    {
        $files = [];

        foreach (native_local_filesystem()->list($directory->suffix('/**/*')) as $status) {
            if ($status->isFile()) {
                $files[] = substr($status->path->uri(), strlen($directory->uri()) + 1);
            }
        }

        sort($files);

        return $files;
    }

    /**
     * @return list<string> every temporary output directory under $root, at any depth, relative, sorted
     */
    public static function temporaryOutputs(Path $root): array
    {
        if (!is_dir($root->path())) {
            return [];
        }

        $directories = [];
        $entries = new RecursiveIteratorIterator(
            new RecursiveDirectoryIterator($root->path(), RecursiveDirectoryIterator::SKIP_DOTS),
            RecursiveIteratorIterator::SELF_FIRST,
        );

        foreach ($entries as $entry) {
            if (
                $entry instanceof SplFileInfo
                && $entry->isDir()
                && str_starts_with($entry->getFilename(), '.flow-tmp-')
            ) {
                $directories[] = substr($entry->getPathname(), strlen($root->path()) + 1);
            }
        }

        sort($directories);

        return $directories;
    }
}
