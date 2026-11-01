<?php

declare(strict_types=1);

namespace Flow\ETL\IO\Filesystem;

use Flow\Filesystem\FileStatus;
use Flow\Filesystem\Path;
use Flow\Filesystem\Path\Filter;

use function explode;
use function in_array;
use function rtrim;
use function str_contains;
use function str_starts_with;
use function strlen;
use function substr;

/**
 * Skips files under a `.` or `_` path segment below the named root, except partition and `_metadata` segments.
 */
final readonly class VisibleFiles implements Filter
{
    private string $root;

    public function __construct(Path $root)
    {
        $this->root = rtrim($root->uri(), '/') . '/';
    }

    public function accept(FileStatus $status): bool
    {
        if (!$status->isFile()) {
            return false;
        }

        $uri = $status->path->uri();

        if (!str_starts_with($uri, $this->root)) {
            // the root is the file itself
            return true;
        }

        foreach (explode('/', substr($uri, strlen($this->root))) as $segment) {
            if (
                ($segment[0] ?? '') !== '.' && ($segment[0] ?? '') !== '_'
                || str_contains($segment, '=')
                || in_array($segment, ['_metadata', '_common_metadata'], true)
            ) {
                continue;
            }

            return false;
        }

        return true;
    }
}
