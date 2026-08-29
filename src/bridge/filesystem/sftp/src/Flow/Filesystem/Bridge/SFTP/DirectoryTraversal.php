<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\SFTP;

use Flow\Filesystem\Bridge\SFTP\DirectoryTraversal\PathPattern;
use Flow\Filesystem\Bridge\SFTP\DirectoryTraversal\RemoteEntries;
use Flow\Filesystem\FileStatus;
use Flow\Filesystem\Path;
use Flow\Filesystem\Path\Filter;
use Generator;
use phpseclib3\Net\SFTP;

use function rtrim;

final readonly class DirectoryTraversal
{
    public function __construct(
        private SFTP $sftp,
        private SFTPSession $session,
    ) {}

    /**
     * @return Generator<int, FileStatus>
     */
    public function walk(Path $path, Filter $pathFilter): Generator
    {
        $pattern = new PathPattern($path);
        $root = RemotePath::from($path->isPattern() ? $path->staticPart() : $path);

        yield from $this->descend($root->withoutTrailingSlash(), $pattern, $pathFilter);
    }

    /**
     * @return Generator<int, FileStatus>
     */
    private function descend(string $directory, PathPattern $pattern, Filter $pathFilter): Generator
    {
        $entries = RemoteEntries::in($this->sftp, $directory);

        if ($entries === null) {
            $this->session->assertAlive('list ' . $directory);

            return;
        }

        $parent = rtrim($directory, '/');

        foreach ($entries as $entry) {
            $remotePath = $parent . '/' . $entry->name;
            $entryPath = $pattern->pathTo($remotePath);

            if ($entry->isDirectory()) {
                if ($pattern->mayContainMatches($remotePath)) {
                    yield from $this->descend($remotePath, $pattern, $pathFilter);
                }

                if ($pattern->accepts($entryPath)) {
                    $directoryStatus = new FileStatus($entryPath, false);

                    if ($pathFilter->accept($directoryStatus)) {
                        yield $directoryStatus;
                    }
                }

                continue;
            }

            if (!$pattern->accepts($entryPath)) {
                continue;
            }

            $fileStatus = new FileStatus($entryPath, true, $entry->size, $entry->lastModifiedAt);

            if ($pathFilter->accept($fileStatus)) {
                yield $fileStatus;
            }
        }
    }
}
