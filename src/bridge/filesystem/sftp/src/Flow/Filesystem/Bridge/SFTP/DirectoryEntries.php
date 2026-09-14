<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\SFTP;

use Generator;
use IteratorAggregate;

/**
 * @implements \IteratorAggregate<int, DirectoryEntry>
 */
final readonly class DirectoryEntries implements IteratorAggregate
{
    public function __construct(
        private ?DirectoryEntry $entry = null,
        private ?DirectoryEntries $following = null,
    ) {}

    /**
     * @return Generator<int, DirectoryEntry>
     */
    public function getIterator(): Generator
    {
        $entries = $this;

        while ($entries !== null && $entries->entry !== null) {
            yield $entries->entry;

            $entries = $entries->following;
        }
    }
}
