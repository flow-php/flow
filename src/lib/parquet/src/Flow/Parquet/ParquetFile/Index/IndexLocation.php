<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\Index;

use Flow\Parquet\Exception\InvalidArgumentException;

/**
 * Where a column chunk's column index or offset index is written.
 */
final readonly class IndexLocation
{
    /**
     * @var positive-int
     */
    public int $length;

    /**
     * @var non-negative-int
     */
    public int $offset;

    public function __construct(int $offset, int $length)
    {
        if ($offset < 0 || $length < 1) {
            throw new InvalidArgumentException(
                'An index location needs an offset of at least 0 and a length of at least 1, got '
                . $offset
                . ' and '
                . $length,
            );
        }

        $this->offset = $offset;
        $this->length = $length;
    }

    /**
     * Null when the chunk has no such index, or a location no index can be read from.
     */
    public static function fromThrift(int|string|null $offset, int|string|null $length): ?self
    {
        if ($offset === null || $length === null || (int) $offset < 0 || (int) $length < 1) {
            return null;
        }

        return new self((int) $offset, (int) $length);
    }
}
