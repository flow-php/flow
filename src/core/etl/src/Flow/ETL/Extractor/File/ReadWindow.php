<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor\File;

use Flow\ETL\Exception\InvalidArgumentException;

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
}
