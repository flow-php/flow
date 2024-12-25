<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\AsyncAWS\AsyncAWSS3SourceStream;

final class Range
{
    /**
     * Constructor to initialize offset and limit.
     *
     * @param null|int $offset the starting byte position (nullable for reading from the end)
     * @param null|int $limit the number of bytes to read (nullable for reading until the end)
     */
    public function __construct(private readonly ?int $offset = null, private readonly ?int $limit = null)
    {
        if ($offset !== null && $offset < 0) {
            throw new \InvalidArgumentException('Offset must be >= 0 if provided, $offset provided: ' . $offset);
        }

        if ($limit !== null && $limit <= 0) {
            throw new \InvalidArgumentException('Limit must be > 0 if provided, $limit provided: ' . $limit);
        }
    }

    /**
     * Returns the Range header string.
     *
     * @return string the formatted Range header
     */
    public function toString() : string
    {
        if ($this->offset === null && $this->limit === null) {
            return '';
        }

        if ($this->offset === null) {
            // Read from the end of the file
            return "bytes=-{$this->limit}";
        }

        if ($this->limit === null) {
            // Read from offset to the end of the file
            return "bytes={$this->offset}-";
        }

        $end = $this->offset + $this->limit - 1;

        return "bytes={$this->offset}-{$end}";
    }
}
