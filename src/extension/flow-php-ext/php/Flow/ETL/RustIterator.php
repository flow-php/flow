<?php

declare(strict_types=1);

namespace Flow\ETL;

use Iterator;
use RuntimeException;

use function extension_loaded;

if (extension_loaded('flow_php')) {
    return;
}

/**
 * Streams once: rewind() after next() throws \Flow\ETL\Exception\RuntimeException('RustIterator cannot rewind').
 *
 * @template-covariant T
 *
 * @implements Iterator<int, T>
 */
final class RustIterator implements Iterator
{
    /**
     * @return T
     */
    public function current(): mixed
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }

    public function key(): ?int
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }

    public function next(): void
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }

    public function rewind(): void
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }

    public function valid(): bool
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }
}
