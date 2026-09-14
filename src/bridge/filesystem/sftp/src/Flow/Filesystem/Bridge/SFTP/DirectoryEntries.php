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
    /**
     * @param array<int, DirectoryEntry> $entries
     */
    public function __construct(
        private array $entries = [],
    ) {}

    /**
     * @return Generator<int, DirectoryEntry>
     */
    public function getIterator(): Generator
    {
        foreach ($this->entries as $entry) {
            yield $entry;

            if ($entry->content !== null) {
                yield from $entry->content;
            }
        }
    }
}
