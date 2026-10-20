<?php

declare(strict_types=1);

namespace Flow\Parquet\Reader;

use Flow\Parquet\Exception\RuntimeException;

/**
 * Adjacent pages of a column chunk read in one SourceStream::read: the file bytes [start, end).
 */
final readonly class PageRun
{
    /**
     * @var positive-int
     */
    public int $length;

    public function __construct(
        public int $start,
        public int $end,
    ) {
        $length = $end - $start;

        if ($length < 1) {
            throw new RuntimeException('The offset index locates a page of no bytes at offset ' . $start);
        }

        $this->length = $length;
    }

    public function extendedTo(int $end): self
    {
        return new self($this->start, $end);
    }
}
