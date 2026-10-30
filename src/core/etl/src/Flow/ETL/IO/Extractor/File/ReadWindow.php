<?php

declare(strict_types=1);

namespace Flow\ETL\IO\Extractor\File;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Values\Rows;

final readonly class ReadWindow
{
    /**
     * @param int $offset rows still to skip
     * @param null|int $limit rows still wanted, null for all of them
     *
     * @throws InvalidArgumentException
     */
    public function __construct(
        public int $offset = 0,
        public ?int $limit = null,
    ) {
        if ($offset < 0) {
            throw InvalidArgumentException::because('ReadWindow offset must be 0 or more, got %d', $offset);
        }

        if ($limit !== null && $limit < 0) {
            throw InvalidArgumentException::because('ReadWindow limit must be 0 or more, got %d', $limit);
        }
    }

    /**
     * $rows without what the offset still skips after $skipped rows.
     */
    public function skip(Rows $rows, int $skipped): Rows
    {
        $rest = $this->offset - $skipped;

        if ($rest <= 0) {
            return $rows;
        }

        return $rest >= $rows->count() ? $rows->slice(0, 0) : $rows->slice($rest, $rows->count() - $rest);
    }

    /**
     * $rows cut to what the limit still wants after $yielded rows.
     */
    public function trim(Rows $rows, int $yielded): Rows
    {
        return $this->limit !== null && $rows->count() > ($this->limit - $yielded)
            ? $rows->slice(0, $this->limit - $yielded)
            : $rows;
    }

    public function reached(int $yielded): bool
    {
        return $this->limit !== null && $yielded >= $this->limit;
    }
}
