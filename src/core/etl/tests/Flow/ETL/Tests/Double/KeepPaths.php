<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\Filesystem\FileStatus;
use Flow\Filesystem\Path;
use Flow\Filesystem\Path\Filter;

use function array_map;
use function in_array;

/**
 * A pushed path filter that keeps only the given files, the way a partition filter narrows a listing.
 */
final readonly class KeepPaths implements Filter
{
    /**
     * @var list<string>
     */
    private array $uris;

    /**
     * @param list<Path> $paths
     */
    public function __construct(array $paths)
    {
        $this->uris = array_map(static fn(Path $path): string => $path->uri(), $paths);
    }

    public function accept(FileStatus $status): bool
    {
        return $status->isFile() && in_array($status->path->uri(), $this->uris, true);
    }
}
